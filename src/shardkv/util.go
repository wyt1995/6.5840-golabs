package shardkv

import "log"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func copyMap[K comparable, V any](original map[K]V) map[K]V {
	newMap := make(map[K]V)
	for k, v := range original {
		newMap[k] = v
	}
	return newMap
}
