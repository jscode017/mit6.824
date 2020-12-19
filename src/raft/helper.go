package raft

import (
	"math/rand"
)

func getRandTime(minTime, maxTime int) int {
	return minTime + rand.Intn(maxTime-minTime)
}
