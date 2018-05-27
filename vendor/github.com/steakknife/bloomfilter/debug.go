//
// Face-meltingly fast, thread-safe, marshalable, unionable, probability- and optimal-size-calculating Bloom filter in go
//
// https://github.com/steakknife/bloomfilter
//
// Copyright © 2014, 2015 Barry Allard
//
// MIT license
//
package bloomfilter

import "log"

var Debug = false

func debug(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func panicf(format string, a ...interface{}) {
	log.Panicf(format, a...)
}
