package main

import (
	"context"
	"flag"
	"fmt"

	"log"
	"math/rand"
	"os"
	"time"

	pb "example.com/query"
	"example.com/util"
	"google.golang.org/grpc"
)

var DBSize uint64
var DBSeed uint64
var serverAddr string
var LogFile *os.File
var ignorePreprocessing bool

// ----------------------------------------------------------------------------

var rng *rand.Rand
var s0 uint32
var s1 uint32
var kappa uint32
var hintNum uint32
var _k uint32
var throwNum uint32 = 1
var cacheNum uint32

// Cached divisors for speed (set in setParameter)
var base0 uint32 // DBSize / s0
var base1 uint32 // DBSize / s1
var d0 uint32    // DBSize / s0 / s1
var d1 uint32    // DBSize / s1

type LocalHint struct {
	row0        []uint32
	row1        []uint32
	parity      []util.DBEntry
	brokenPoint uint32
}

func randomHint() LocalHint {
	var hint LocalHint
	hint.row0 = make([]uint32, s0)
	hint.row1 = make([]uint32, s1)
	hint.parity = make([]util.DBEntry, s0)
	for j := uint32(0); j < s0; j++ {
		r := rng.Uint32() % base0
		val := (r / s1) + (j*base0)/s1
		hint.row0[j] = uint32(val)
		hint.parity[j] = util.ZeroEntry()
	}
	for j := uint32(0); j < s1; j++ {
		r := rng.Uint32() % base1
		val := r + j*base1
		hint.row1[j] = uint32(val)
	}
	hint.brokenPoint = rng.Uint32() % s0
	return hint
}
func row1Idx(x uint32) uint32 {
	return x / d1
}
func row0Idx(hint *LocalHint, x uint32) uint32 {
	num := x ^ hint.row1[row1Idx(x)]
	return num / d0
}
func membershipTest(hint *LocalHint, x uint32) bool {
	var x1 = x ^ (hint.row1[x/d1])
	var x0 = x1 ^ (hint.row0[x1/d0])
	return x0 == 0
}

func setParameter() {
	// first two cases for ease of comparing with QuarterPIR
	if util.DBEntrySize == 32 && DBSize == 4294967296 {
		s0 = 2048
		s1 = 16384
	} else if util.DBEntrySize == 1024 && DBSize == 134217728 {
		s0 = 512
		s1 = 8192
	} else {
		var C = uint64(1)
		for C = 1; C*C <= util.DBEntrySize/4; C *= 2 {
		}
		C /= 2
		for s0 = 1; uint64(s0)*uint64(s0)*uint64(s0) <= DBSize; s0 *= 2 {
		}
		s0 *= 2 // to account for the k factor blowup in storage; not discussed in paper
		s1 = s0 * uint32(C)
		s0 = s0 / uint32(C)
	}
	k := uint32(0)
	// The parameters below ensure a total correctness error of 2^{-40} per query
	if s0 >= 65536 { // 2**16
		k = 39
		kappa = 4
		throwNum = 1
	} else if s0 == 32768 { // 2**15
		k = 40
		kappa = 4
		throwNum = 1
	} else if s0 == 8192 { // 2**13
		k = 42
		kappa = 5
		throwNum = 1
	} else if s0 == 4096 { // 2**12
		k = 42
		kappa = 5
		throwNum = 2
	} else if s0 == 2048 { // 2**11
		k = 44
		kappa = 6
		throwNum = 2
	} else if s0 == 1024 { // 2**10
		k = 44
		kappa = 6
		throwNum = 3
	} else if s0 == 512 { // 2**9
		k = 46
		kappa = 7
		throwNum = 3
	} else if s0 == 256 { // 2**8
		k = 47
		kappa = 7
		throwNum = 4
	} else if s0 == 128 { // 2**7
		k = 49
		kappa = 8
		throwNum = 5
	} else if s0 == 64 { // 2**6
		k = 51
		kappa = 9
		throwNum = 6
	} else if s0 == 32 { // 2**5
		k = 53
		kappa = 10
		throwNum = 9
	} else {
		log.Fatalf("Error: s0 = %v is not supported", s0)
	}
	_k = k
	// Cache divisors and validate once
	base0 = uint32(DBSize / uint64(s0))
	base1 = uint32(DBSize / uint64(s1))
	d1 = base1
	d0 = base0 / s1
	if d0 == 0 {
		log.Fatalf("invalid parameters: (DBSize/s0)/s1 = 0 (DBSize=%d, s0=%d, s1=%d)", DBSize, s0, s1)
	}
	hintNum = uint32(uint64(k) * DBSize / (uint64(s0) * uint64(s1)))
	cacheNum = (hintNum-1)/throwNum + 1
}

func refreshReplacementEntries(Server pb.QueryServiceClient, ctx context.Context, replacementIndices []uint32, replacementValues []util.DBEntry) (uint64, uint64, uint64) {
	networkStart := time.Now()
	ReplacementEntryQueryMsg := &pb.ReplacementEntryQueryMsg{S1: s1}
	res, err := Server.ReplacementEntryQuery(ctx, ReplacementEntryQueryMsg)
	if err != nil {
		log.Fatalf("failed to make replacement entry query to server %v", err)
	}
	networkElapsed := time.Since(networkStart)
	start := time.Now()
	for i := uint32(0); i < s1; i++ {
		replacementIndices[i] = res.Index[i]
		replacementValues[i] = util.DBEntryFromSlice(res.Parity[uint64(i)*util.DBEntryLength : uint64(i+1)*util.DBEntryLength])
	}
	elapsed := time.Since(start)
	return uint64(elapsed.Nanoseconds()), res.ServerComputeTime, uint64(networkElapsed.Nanoseconds())
}

func runPIR(Server pb.QueryServiceClient, DBSize uint64, DBSeed uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Millisecond*100000000000))
	defer cancel()
	seed := time.Now().UnixNano()
	rng = rand.New(rand.NewSource(seed))

	totalQueryNum := uint32(1000)
	log.Printf("totalQueryNum %v", totalQueryNum)

	// calculate number of hints and size of hints
	setParameter()

	hintSize := 4*s0 + 4*s1 + util.DBEntrySize*s0 + 4
	hintTotalSize := hintNum * hintSize
	replacementSize := (4 + util.DBEntrySize) * s1
	cacheSize := cacheNum * util.DBEntrySize
	totalLocalStorage := hintTotalSize + replacementSize + cacheSize
	onlineUploadSize := kappa * 4 * s1
	onlineDownloadSize := kappa * util.DBEntrySize
	offlineDownloadSize := (kappa+throwNum)*hintSize + replacementSize

	start := time.Now()

	// Initialize hints
	hints := make([]LocalHint, hintNum)
	replacementIndices := make([]uint32, s1)
	replacementValues := make([]util.DBEntry, s1)
	localCache := make(map[uint32]util.DBEntry)
	queue := make([]uint32, 0)

	log.Printf("s0 = %v, s1 = %v, hintNum = %v, kappa = %v, throwNum = %v", s0, s1, hintNum, kappa, throwNum)
	log.Printf("Every Local Hint Size %v KB, in total %v MB", hintSize>>10, hintTotalSize>>20)
	log.Printf("Total Replacement Entry Size %v MB", replacementSize>>20)
	log.Printf("Total Cache Size %v MB", cacheSize>>20)
	log.Printf("Total Local Memory %v MB", totalLocalStorage>>20)
	log.Printf("Per query online upload communication cost %v kb", onlineUploadSize>>10)
	log.Printf("Per query online download communication cost %v kb", onlineDownloadSize>>10)
	log.Printf("Per query offline communication cost %v kb", offlineDownloadSize>>10)

	if !ignorePreprocessing {
		Server.HintComputeTimeQuery(ctx, &pb.HintComputeTimeQueryMsg{}) // clear cache and compute time
		RandomHintQueryMsg := &pb.RandomHintQueryMsg{S0: s0, S1: s1, Num: hintNum}
		stream, err := Server.RandomHintQuery(ctx, RandomHintQueryMsg)
		if err != nil {
			log.Fatalf("failed to make random hint query to server %v", err)
		}
		for i := uint32(0); i < hintNum; i++ {
			res, err := stream.Recv()
			if err != nil {
				log.Fatalf("failed to receive a hint %v", err)
			}
			hints[i].row0 = res.Row0
			hints[i].row1 = res.Row1
			hints[i].parity = make([]util.DBEntry, s0)
			for j := 0; j < int(s0); j++ {
				hints[i].parity[j] = util.DBEntryFromSlice(res.Parity[uint64(j)*util.DBEntryLength : uint64(j+1)*util.DBEntryLength])
			}
			hints[i].brokenPoint = s0 // not broken
		}
	} else {
		for i := uint32(0); i < hintNum; i++ {
			hints[i] = randomHint()
		}
	}

	refreshReplacementEntries(Server, ctx, replacementIndices, replacementValues)

	preprocessingElapsed := time.Since(start)

	log.Printf("Finish Preprocessing Phase")
	log.Printf("Preprocessing Phase took %v ms", preprocessingElapsed.Milliseconds())

	Server.HintComputeTimeQuery(ctx, &pb.HintComputeTimeQueryMsg{}) // clear cache and compute time

	// Online Query Phase:
	totalOnlineNetworkLatency := uint64(0)
	totalOfflineNetworkLatency := uint64(0)
	totalOnlineServerComputeTime := uint64(0)
	totalOfflineServerComputeTime := uint64(0)
	totalOnlineClientComputeTime := uint64(0)
	totalOfflineClientComputeTime := uint64(0)

	start = time.Now()

	I := uint32(0)
	for q := uint32(0); q < totalQueryNum; q++ {
		if q%100 == 0 {
			log.Printf("Making %v-th query", q)
		}
		// just do random query for now
		x := uint32(rng.Uint64() % DBSize)

		// make sure x is not in the local cache
		for {
			if _, ok := localCache[x]; !ok {
				break
			}
			x = uint32(rng.Uint64() % DBSize)
		}

		// find all hit sets and randomly select kappa
		requestStart := time.Now()
		var hitId = make([]uint32, 0, 2*_k)
		for i := uint32(0); i < hintNum; i++ {
			if membershipTest(&hints[i], x) {
				hitId = append(hitId, i)
			}
		}
		if uint32(len(hitId)) < kappa {
			log.Fatalf("Error: cannot find enough hitId")
		}
		rng.Shuffle(len(hitId), func(i, j int) { hitId[i], hitId[j] = hitId[j], hitId[i] })
		hitId = hitId[:kappa]
		var OkId = uint32(kappa)
		for i := uint32(0); i < kappa; i++ {
			if hints[hitId[i]].brokenPoint != row0Idx(&hints[hitId[i]], x) {
				OkId = i
				break
			}
		}
		if OkId == kappa {
			log.Fatalf("Error: hints all broken")
		}

		querySet := make([]uint32, kappa*s1)
		row1Id := row1Idx(x)
		for i := uint32(0); i < kappa; i++ {
			row0 := x ^ hints[hitId[i]].row1[row1Id]
			for j := uint32(0); j < s1; j++ {
				querySet[i*s1+j] = hints[hitId[i]].row1[j] ^ row0
			}
			if i == OkId {
				querySet[i*s1+row1Id] = replacementIndices[row1Id]
			} else {
				rnd := rng.Uint32() % base1
				val := rnd + row1Id*base1
				querySet[i*s1+row1Id] = val
			}
		}

		totalOnlineClientComputeTime += uint64(time.Since(requestStart).Nanoseconds())

		// send the edited set to the server
		networkStart := time.Now()
		res, err := Server.SetParityQuery(ctx, &pb.SetParityQueryMsg{SetNum: kappa, SetSize: s1, Indices: querySet})
		networkSince := time.Since(networkStart)
		if err != nil {
			log.Fatalf("failed to make punct set query to server %v", err)
		}

		totalOnlineNetworkLatency += uint64(networkSince.Nanoseconds()) - res.ServerComputeTime
		totalOnlineServerComputeTime += res.ServerComputeTime

		// calculate answer
		var xVal util.DBEntry = hints[hitId[OkId]].parity[row0Idx(&hints[hitId[OkId]], x)]
		util.DBEntryXor(&xVal, &replacementValues[row1Id])
		util.DBEntryXorFromRaw(&xVal, res.Parity[uint64(OkId)*util.DBEntryLength:uint64(OkId+1)*util.DBEntryLength])

		// verify the correctness of the query
		entry := util.GenDBEntry(DBSeed, uint64(x))
		// if ignorePreprocessing == true, the client will not verify the correctness of the query
		if !ignorePreprocessing && !util.EntryIsEqual(&xVal, &entry) {
			log.Fatalf("wrong value %v at index %v at query %v", xVal, x, q)
		}

		// update the local cache
		localCache[x] = xVal
		if len(queue) >= int(cacheNum) {
			delete(localCache, queue[0])
			queue = queue[1:]
		}
		queue = append(queue, x)

		clientTime, serverTime, networkTime := refreshReplacementEntries(Server, ctx, replacementIndices, replacementValues)
		totalOfflineClientComputeTime += clientTime
		totalOfflineServerComputeTime += serverTime
		totalOfflineNetworkLatency += networkTime

		refreshNetworkStart := time.Now()
		RandomHintQueryMsg := &pb.RandomHintQueryMsg{S0: s0, S1: s1, Num: kappa + throwNum}
		stream, err := Server.RandomHintQuery(ctx, RandomHintQueryMsg)
		if err != nil {
			log.Fatalf("failed to make random hint query to server %v", err)
		}
		recvHints := make([]*pb.RandomHintResponse, kappa+throwNum)
		for i := uint32(0); i < kappa+throwNum; i++ {
			res, err := stream.Recv()
			if err != nil {
				log.Fatalf("failed to receive a hint %v %v", err, i+1)
			}
			recvHints[i] = res
		}
		totalOfflineNetworkLatency += uint64(time.Since(refreshNetworkStart).Nanoseconds())

		offlineClientStart := time.Now()
		for i := uint32(0); i < throwNum; i++ {
			hitId = append(hitId, I)
			I = (I + 1) % hintNum
		}
		for ii := uint32(0); ii < kappa+throwNum; ii++ {
			res := recvHints[ii]
			i := hitId[ii]
			hints[i].row0 = res.Row0
			hints[i].row1 = res.Row1
			hints[i].parity = make([]util.DBEntry, s0)
			for j := 0; j < int(s0); j++ {
				hints[i].parity[j] = util.DBEntryFromSlice(res.Parity[uint64(j)*util.DBEntryLength : uint64(j+1)*util.DBEntryLength])
			}
			if ii < kappa {
				row1Id := row1Idx(x)
				row0Id := row0Idx(&hints[i], x)
				hints[i].row0[row0Id] = x ^ hints[i].row1[row1Id]
				hints[i].brokenPoint = row0Id
			} else {
				hints[i].brokenPoint = s0 // not broken
			}
		}
		totalOfflineClientComputeTime += uint64(time.Since(offlineClientStart).Nanoseconds())
	}

	totalEndToEndTime := time.Since(start)
	res, err := Server.HintComputeTimeQuery(ctx, &pb.HintComputeTimeQueryMsg{})
	if err != nil {
		log.Fatalf("failed to get hint compute time from server %v", err)
	}
	totalOfflineServerComputeTime += res.HintComputeTime
	totalOfflineNetworkLatency -= res.HintComputeTime

	// assert that totalEndToEndTime is near equal to sum of all components
	totalEndToEndMs := float64(totalEndToEndTime.Milliseconds())
	totalComponentsMs := float64(totalOnlineNetworkLatency+totalOfflineNetworkLatency+totalOnlineServerComputeTime+totalOfflineServerComputeTime+totalOnlineClientComputeTime+totalOfflineClientComputeTime) / 1000000
	if totalEndToEndMs/totalComponentsMs > 1.01 || totalEndToEndMs/totalComponentsMs < 0.99 {
		log.Printf("WARNING: totalEndToEndTime does not match sum of components")
	}

	avgOnlineNetworkLatency := float64(totalOnlineNetworkLatency) / float64(totalQueryNum)         // only latency in online phase
	avgOfflineNetworkLatency := float64(totalOfflineNetworkLatency) / float64(totalQueryNum)       // only latency in online phase
	avgOnlineServerComputeTime := float64(totalOnlineServerComputeTime) / float64(totalQueryNum)   // single-thread server
	avgOfflineServerComputeTime := float64(totalOfflineServerComputeTime) / float64(totalQueryNum) // multi-thread server
	avgOnlineClientComputeTime := float64(totalOnlineClientComputeTime) / float64(totalQueryNum)   // only online client compute time
	avgOfflineClientComputeTime := float64(totalOfflineClientComputeTime) / float64(totalQueryNum) // only offline client compute time

	log.Printf("Finish Online Phase with %v queries", totalQueryNum)
	log.Printf("Online Phase took %v ms, amortized time %v ms", totalEndToEndTime.Milliseconds(), float64(totalEndToEndTime.Milliseconds())/float64(totalQueryNum))
	log.Printf("End to end amortized time %v ms", float64(totalEndToEndTime.Milliseconds())/float64(totalQueryNum))

	log.Printf("---------------breakdown-------------------------")
	log.Printf("End to end amortized time %v ms", float64(totalEndToEndTime.Milliseconds())/float64(totalQueryNum))
	log.Printf("Average Online Network Latency %v ms", avgOnlineNetworkLatency/1000000)
	log.Printf("Average Offline Network Latency %v ms", avgOfflineNetworkLatency/1000000)
	log.Printf("Average Online Server Time %v ms", avgOnlineServerComputeTime/1000000)
	log.Printf("Average Offline Server Time %v ms", avgOfflineServerComputeTime/1000000)
	log.Printf("Average Online Client Time %v ms", avgOnlineClientComputeTime/1000000)
	log.Printf("Average Offline Client Time %v ms", avgOfflineClientComputeTime/1000000)
	log.Printf("-------------------------------------------------")

	LogFile.WriteString("--------------------stats--------------------------\n")
	str := fmt.Sprintf("Current Time: %v", time.Now().Format("2006-01-02 15:04:05"))
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("DB n: %v", DBSize)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("DB entry size (Bytes): %v", util.DBEntrySize)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("DB size (GB): %v", float64(DBSize)*float64(util.DBEntrySize)/1024/1024/1024)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("s0 = %v, s1 = %v, hintNum = %v, kappa = %v, throwNum = %v", s0, s1, hintNum, kappa, throwNum)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Every Local Hint Size %v KB, in total %v MB", hintSize>>10, hintTotalSize>>20)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Total Replacement Entry Size %v MB", replacementSize>>20)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Total Cache Size %v MB", cacheSize>>20)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Total Local Memory %v MB", totalLocalStorage>>20)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Per query online upload communication cost %v kb", onlineUploadSize>>10)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Per query online download communication cost %v kb", onlineDownloadSize>>10)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Per query offline communication cost %v kb", offlineDownloadSize>>10)
	LogFile.WriteString(str + "\n")

	if !ignorePreprocessing {
		str = fmt.Sprintf("Preprocessing Phase took %v ms", preprocessingElapsed.Milliseconds())
		LogFile.WriteString(str + "\n")
	}

	str = fmt.Sprintf("End to end amortized time %v ms", float64(totalEndToEndTime.Milliseconds())/float64(totalQueryNum))
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Online Network Latency %v ms", avgOnlineNetworkLatency/1000000)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Offline Network Latency %v ms", avgOfflineNetworkLatency/1000000)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Online Server Time %v ms", avgOnlineServerComputeTime/1000000)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Offline Server Time %v ms", avgOfflineServerComputeTime/1000000)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Online Client Time %v ms", avgOnlineClientComputeTime/1000000)
	LogFile.WriteString(str + "\n")

	str = fmt.Sprintf("Average Offline Client Time %v ms", avgOfflineClientComputeTime/1000000)
	LogFile.WriteString(str + "\n")

	LogFile.WriteString("---------------------------------------------------\n\n\n")
}

func main() {
	addrPtr := flag.String("ip", "localhost:50051", "port number")
	ignorePreprocessingPtr := flag.Bool("ignorePreprocessing", false, "ignore preprocessing phase")
	flag.Parse()

	serverAddr = *addrPtr
	ignorePreprocessing = *ignorePreprocessingPtr
	log.Printf("Server address %v, ignorePreprocessing %v", serverAddr, ignorePreprocessing)

	DBSize = util.DBSize
	DBSeed = util.DBSeed

	maxMsgSize := 64 * 1024 * 1024

	f, _ := os.OpenFile("output.txt", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	LogFile = f

	log.Printf("DBSize %v, DBSeed %v, DB entrysize %v B, DB storage %v GB", DBSize, DBSeed, util.DBEntrySize, DBSize*util.DBEntrySize/1024/1024/1024)

	Conn, err := grpc.Dial(
		serverAddr,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsgSize), grpc.MaxCallSendMsgSize(maxMsgSize)),
	)
	if err != nil {
		log.Fatalf("Failed to connect server")
	}
	Server := pb.NewQueryServiceClient(Conn)

	defer Conn.Close()

	runPIR(Server, DBSize, DBSeed)
}
