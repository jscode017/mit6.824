package raft

import (
	"math/rand"
)

func getRandTime(minTime, maxTime int) int {
	return minTime + rand.Intn(maxTime-minTime)
}
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
