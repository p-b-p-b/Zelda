package main

import (
	"context"
	"flag"
	"log"
	"math/bits"
	"net"
	"sync"
	"time"

	pb "example.com/query"
	util "example.com/util"
	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
)

var DBSize uint64
var DBSeed uint64
var port string
var rng *rand.Rand
var numThreads uint32

type QueryServiceServer struct {
	pb.UnimplementedQueryServiceServer
	DB []uint64 // the database, for every DBEntrySize/8 uint64s, we store a DBEntry
}

func (s *QueryServiceServer) DBAccess(id uint32) util.DBEntry {
	if uint64(id) < uint64(len(s.DB)) {
		if uint64(id)*util.DBEntryLength+util.DBEntryLength > uint64(len(s.DB)) {
			log.Fatalf("DBAccess: id %d out of range", id)
		}
		return util.DBEntryFromSlice(s.DB[uint64(id)*util.DBEntryLength : (uint64(id)+1)*util.DBEntryLength])
	} else {
		log.Fatalf("DBAccess: id %d out of range [%v]", id, len(s.DB))
		var ret util.DBEntry
		for i := 0; i < util.DBEntryLength; i++ {
			ret[i] = 0
		}
		return ret
	}
}

func (s *QueryServiceServer) DBAccessSlice(id uint32) []uint64 {
	if uint64(id) >= uint64(len(s.DB)) || uint64(id)*util.DBEntryLength+util.DBEntryLength > uint64(len(s.DB)) {
		log.Fatalf("DBAccess: id %d out of range", id)
	}
	return s.DB[uint64(id)*util.DBEntryLength : (uint64(id)+1)*util.DBEntryLength]
}

func (s *QueryServiceServer) PlaintextQuery(ctx context.Context, in *pb.PlaintextQueryMsg) (*pb.PlaintextResponse, error) {
	id := in.GetIndex()
	ret := s.DBAccess(id)
	return &pb.PlaintextResponse{Val: ret[:]}, nil
}

/*
 * The client sends the indices of many sets.
 * The server returns the parity of many sets.
 */

func (s *QueryServiceServer) HandleSetParityQuery(num uint32, size uint32, indices []uint32) []uint64 {
	parity := make([]uint64, num*util.DBEntryLength)
	for ii, index := range indices {
		entry := s.DBAccessSlice(index)
		i := uint64(ii) / uint64(size)
		util.SliceXor(parity[i*util.DBEntryLength:(i+1)*util.DBEntryLength], entry)
	}
	return parity
}

func (s *QueryServiceServer) SetParityQuery(ctx context.Context, in *pb.SetParityQueryMsg) (*pb.SetParityQueryResponse, error) {
	start := time.Now()
	parity := s.HandleSetParityQuery(in.GetSetNum(), in.GetSetSize(), in.GetIndices())
	since := time.Since(start)
	return &pb.SetParityQueryResponse{Parity: parity, ServerComputeTime: uint64(since.Nanoseconds())}, nil
}

var singlepass uint32 = 256
var hintQueue = make([]*pb.RandomHintResponse, 0, singlepass*2)
var totalHintComputeTime uint64

func (s *QueryServiceServer) HintComputeTimeQuery(ctx context.Context, in *pb.HintComputeTimeQueryMsg) (*pb.HintComputeTimeResponse, error) {
	res := totalHintComputeTime
	totalHintComputeTime = 0
	hintQueue = make([]*pb.RandomHintResponse, 0, singlepass*2)
	return &pb.HintComputeTimeResponse{HintComputeTime: res}, nil
}

var s0 uint32
var s1 uint32

func generateHint(s *QueryServiceServer) {
	log.Printf("Generating %v hints", singlepass)
	start := time.Now()
	results := make([]pb.RandomHintResponse, singlepass)
	base0 := DBSize / uint64(s0)
	base1 := DBSize / uint64(s1)
	for i := uint32(0); i < singlepass; i++ {
		results[i].Row0 = make([]uint32, s0)
		results[i].Row1 = make([]uint32, s1)
		for j := uint32(0); j < s0; j++ {
			r := rng.Uint64() % base0
			val := (r / uint64(s1)) + (uint64(j)*base0)/uint64(s1)
			results[i].Row0[j] = uint32(val)
		}
		for j := uint32(0); j < s1; j++ {
			r := rng.Uint64() % base1
			val := r + uint64(j)*base1
			results[i].Row1[j] = uint32(val)
		}
		results[i].Parity = make([]uint64, s0*util.DBEntryLength)
	}

	denom := (DBSize / uint64(s0)) / uint64(s1)
	shift := uint(bits.TrailingZeros64(denom))
	var wg sync.WaitGroup
	wg.Add(int(numThreads))
	for t := uint32(0); t < numThreads; t++ {
		go func(L, R uint32) {
			defer wg.Done()
			for j1 := uint32(0); j1 < s1; j1++ {
				for x0 := uint32(0); x0 < s0; x0++ {
					for i := L; i < R; i++ {
						row1 := results[i].Row1[j1]
						j0 := ((uint32(uint64(row1)>>shift) & (s0 - 1)) ^ x0)
						entry := s.DBAccessSlice(results[i].Row0[j0] ^ row1)
						util.SliceXor(results[i].Parity[j0*util.DBEntryLength:(j0+1)*util.DBEntryLength], entry)
					}
				}
			}
		}(t*(singlepass/numThreads), (t+1)*(singlepass/numThreads))
	}
	wg.Wait()

	for i := uint32(0); i < singlepass; i++ {
		hintQueue = append(hintQueue, &results[i])
	}

	elapsed := time.Since(start)
	log.Printf("Finished generating %v hints, time = %v ms", singlepass, elapsed.Milliseconds())
	totalHintComputeTime += uint64(elapsed.Nanoseconds())
}

func (s *QueryServiceServer) RandomHintQuery(in *pb.RandomHintQueryMsg, stream pb.QueryService_RandomHintQueryServer) error {
	if s0 == 0 || s1 == 0 {
		s0 = in.GetS0()
		s1 = in.GetS1()
		if s0 == 0 || s1 == 0 {
			log.Fatalf("s0 or s1 is 0")
		}
	} else if s0 != in.GetS0() || s1 != in.GetS1() {
		log.Fatalf("s0 or s1 is not consistent")
	}

	sentNum := 0
	for int(in.GetNum())-sentNum > len(hintQueue) {
		for ; len(hintQueue) > 0; hintQueue = hintQueue[:len(hintQueue)-1] {
			if err := stream.Send(hintQueue[len(hintQueue)-1]); err != nil {
				log.Printf("Failed to send a message: %v", err)
				return nil
			}
			sentNum += 1
		}
		generateHint(s)
	}
	for ; sentNum < int(in.GetNum()); hintQueue = hintQueue[:len(hintQueue)-1] {
		if err := stream.Send(hintQueue[len(hintQueue)-1]); err != nil {
			log.Printf("Failed to send a message: %v", err)
			return nil
		}
		sentNum += 1
	}

	return nil
}

func (s *QueryServiceServer) HandleReplacementEntryQuery(in *pb.ReplacementEntryQueryMsg) ([]uint32, []uint64) {
	s1 := in.GetS1()
	index := make([]uint32, s1)
	parity := make([]uint64, s1*util.DBEntryLength)
	for j := uint32(0); j < s1; j++ {
		base := uint32(DBSize / uint64(s1))
		rnd := rng.Uint32() % base
		val := rnd + j*base
		index[j] = val
		entry := s.DBAccess(index[j])
		copy(parity[j*util.DBEntryLength:(j+1)*util.DBEntryLength], entry[:])
	}
	return index, parity
}

func (s *QueryServiceServer) ReplacementEntryQuery(ctx context.Context, in *pb.ReplacementEntryQueryMsg) (*pb.ReplacementEntryResponse, error) {
	start := time.Now()
	index, parity := s.HandleReplacementEntryQuery(in)
	elapsed := time.Since(start)
	return &pb.ReplacementEntryResponse{Index: index, Parity: parity, ServerComputeTime: uint64(elapsed.Nanoseconds())}, nil
}

func main() {
	portPtr := flag.String("port", "50051", "port number")
	ignorePreprocessingPtr := flag.Bool("ignorePreprocessing", false, "ignore preprocessing of the DB")
	numThreadsPtr := flag.Int("numThreads", 8, "number of threads")
	flag.Parse()

	port = *portPtr
	ignorePreprocessing := *ignorePreprocessingPtr
	numThreads = uint32(*numThreadsPtr)
	log.Println("port number: ", port)
	port = ":" + port

	DBSize = util.DBSize
	DBSeed = util.DBSeed
	log.Printf("DB N: %v, Entry Size %v Bytes, DB Size %v MB", DBSize, util.DBEntrySize, uint64(DBSize)*util.DBEntrySize/1024/1024)

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen %v", err)
	}

	DB := make([]uint64, DBSize*util.DBEntryLength)
	log.Println("DB Real N:", len(DB))
	if !ignorePreprocessing {
		for i := uint64(0); i < uint64(len(DB))/util.DBEntryLength; i++ {
			entry := util.GenDBEntry(DBSeed, i)
			copy(DB[i*util.DBEntryLength:(i+1)*util.DBEntryLength], entry[:])
		}
	} else {
		for i := uint64(0); i < uint64(len(DB)); i++ {
			DB[i] = i
		}
	}

	rng = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

	maxMsgSize := 64 * 1024 * 1024

	s := grpc.NewServer(
		grpc.MaxMsgSize(maxMsgSize),
		grpc.MaxRecvMsgSize(maxMsgSize),
		grpc.MaxSendMsgSize(maxMsgSize),
	)

	pb.RegisterQueryServiceServer(s, &QueryServiceServer{DB: DB[:]})
	log.Printf("server listening at %v", lis.Addr())

	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to server %v", err)
	}
}
