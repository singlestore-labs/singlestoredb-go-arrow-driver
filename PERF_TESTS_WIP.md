`go test -bench=. -run=^B`

LOCAL CLIENT

goos: linux
goarch: amd64
pkg: github.com/singlestore-labs/singlestoredb-go-arrow-driver
cpu: 12th Gen Intel(R) Core(TM) i7-1260P

SINGLEBOX
BenchmarkReadRegular-16                        1        7997833847 ns/op
BenchmarkReadArrow-16                          1        8213899218 ns/op
BenchmarkReadArrowServer-16                    3         440983040 ns/op


LOCAL cluster-in-a-box
BenchmarkReadRegular-16                        1        6325171003 ns/op
BenchmarkReadParallel-16                       1        9090986287 ns/op
BenchmarkReadArrow-16                          1        14120213354 ns/op
BenchmarkReadArrowParallel-16                  1        9390935150 ns/op
BenchmarkReadArrowServer-16                    4         275127879 ns/op

S-1 S2MS SERVER
BenchmarkReadRegular-16                        1        4126553651 ns/op
BenchmarkReadParallel-16                       1        5179306474 ns/op
BenchmarkReadArrow-16                          1        4404532083 ns/op
BenchmarkReadArrowParallel-16                  1        4942044003 ns/op
BenchmarkReadArrowServer-16                    1        3442768146 ns/op

AWS CLIENT, S-1 S2MS SERVER
goos: linux
goarch: amd64
pkg: github.com/singlestore-labs/singlestoredb-go-arrow-driver
cpu: Intel(R) Xeon(R) CPU E5-2686 v4 @ 2.30GHz
BenchmarkReadRegular-8               	       2	 756813901 ns/op
BenchmarkReadParallel-8              	       2	 868953042 ns/op
BenchmarkReadArrow-8                 	       2	 943755610 ns/op
BenchmarkReadArrowParallel-8         	       2	 756183224 ns/op
BenchmarkReadArrowServer-8           	       4	 256341378 ns/op
