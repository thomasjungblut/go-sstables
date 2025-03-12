module github.com/thomasjungblut/go-sstables

require (
	github.com/anishathalye/porcupine v0.1.2
	github.com/golang/snappy v0.0.4
	github.com/libp2p/go-buffer-pool v0.1.0
	github.com/ncw/directio v1.0.5
	github.com/steakknife/bloomfilter v0.0.0-20180922174646-6819c0d2a570
	github.com/stretchr/testify v1.9.0
	golang.org/x/exp v0.0.0-20240613232115-7f521ea00fb8
	google.golang.org/protobuf v1.34.2
)

require (
	github.com/bahlo/generic-list-go v0.2.0 // indirect
	github.com/buger/jsonparser v1.1.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/steakknife/hamming v0.0.0-20180906055917-c99c65617cd3 // indirect
	github.com/wk8/go-ordered-map/v2 v2.1.8 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/anishathalye/porcupine v0.1.2 => github.com/tjungblu/porcupine v0.0.0-20221116095144-377185aa0569

go 1.23
