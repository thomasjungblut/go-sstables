[![GoDoc](https://godoc.org/github.com/steakknife/bloomfilter?status.png)](https://godoc.org/github.com/steakknife/bloomfilter)

# Face-meltingly fast, thread-safe, marshalable, unionable, probability- and optimal-size-calculating Bloom filter in go

Copyright © 2014, 2015 Barry Allard

[MIT license](MIT-LICENSE.txt)

## Usage

```go

import "github.com/steakknife/bloomfilter"

const (
  maxElements = 100000
  probCollide = 0.0000001
)

bf := bloomfilter.NewOptimal(maxElements, probCollide)

bf.Add(someValue)
if bf.Contains(someValue) { // probably true, could be false
  // whatever
}

if bf.Contains(anotherValue) {
  panic("This should never happen")
}

err := bf.WriteFile("1.bf.gz")  // saves this BF to a file
if err != nil {
  panic(err)
}

bf2, err := bloomfilter.ReadFile("1.bf.gz") // read the BF to another var
if err != nil {
  panic(err)
}
```


## Design

Where possible, branch-free operations are used to avoid deep pipeline / execution unit stalls on branch misses.

## Get

    go get -u github.com/steakknife/bloomfilter  # master is always stable

## Source

- On the web: https://github.com/steakknife/bloomfilter

- Git: `git clone https://github.com/steakknife/bloomfilter`

## Contact

- [Feedback](mailto:barry.allard@gmail.com)

- [Issues](https://github.com/steakknife/bloomfilter/issues)

## License

[MIT license](MIT-LICENSE.txt)

Copyright © 2014, 2015 Barry Allard
