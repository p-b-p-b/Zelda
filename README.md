# Zelda PIR
This is a prototype implementation of the Zelda PIR, a private information retrieval(PIR) algorithm that allows a client to access a database without the server knowing the querying index.

**Warning**: The code is not audited and not for any serious commercial or real-world use case. Please use it only for educational purpose.

### Prerequisite:
1. Install Go(https://go.dev/doc/install). We use the version of Go 1.19.
2. For developing, please install gRPC(https://grpc.io/docs/languages/go/quickstart/)

### Running Experiments:
0. First run `go mod tidy` to download the dependency.
1. In one terminal, `go run server/server.go -port 50051`. This sets up the server. The server will store the whole DB in the RAM, so please ensure there's enough memory.
2. In another terminal, `go run client/client.go -ip localhost:50051`.
3. View the results in `output.txt`. You can terminate the server process.

The client process will verify the correctness of every query it makes and panic if an error is detected. 
We do so by sharing the `DBSeed` with the client process. Whenever the client finishes one query, it uses the seed to generate the ground truth and compares it to the query answer. The client will not use the seed anywhere else.

#### Different DB configuration:
1. In `util/util.go`, you can change `DBSize` and `DBSeed` to adjust the number of entries in the database, and the seed used for verification.
2. In the same file you can change the `DBEntrySize` constant to update the entry size, e.g. 8 bytes, 32 bytes, 256 bytes.

### Developing
1. The server implementation is in `server/server.go`.
2. The client implementation is in `client/client.go`.
3. Common utilities are in `util/util.go`, including the PRF and the `DBEntry` definition.
4. The messages exchanged by the server and client are defined in `query/query.proto`. If you change it, run `bash proto.sh` to generate the corresponding server and client API. You should implement those API later.

### Contact

`bo.peng [at] stu [dot] pku [dot] edu [dot] cn`
