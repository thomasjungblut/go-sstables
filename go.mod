module github.com/thomasjungblut/go-sstables

require (
	github.com/anishathalye/porcupine v0.1.2
	github.com/godzie44/go-uring v0.0.0-20220926161041-69611e8b13d5
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/libp2p/go-buffer-pool v0.0.2
	github.com/ncw/directio v1.0.5
	github.com/steakknife/bloomfilter v0.0.0-20180922174646-6819c0d2a570
	github.com/stretchr/testify v1.7.0
	golang.org/x/exp v0.0.0-20181210123644-7d6377eee41f
	google.golang.org/protobuf v1.26.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/libp2p/go-sockaddr v0.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/steakknife/hamming v0.0.0-20180906055917-c99c65617cd3 // indirect
	golang.org/x/sys v0.0.0-20210921065528-437939a70204 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.0 // indirect
)

replace github.com/anishathalye/porcupine v0.1.2 => github.com/tjungblu/porcupine v0.0.0-20221116095144-377185aa0569

go 1.19
