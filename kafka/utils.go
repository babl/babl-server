package kafka

import (
	"math/rand"
	"strconv"
	"time"
)

func getRandomID() string {
	randNbr := uint32(random(1, 999999))
	randStr := strconv.FormatUint(uint64(randNbr), 10)
	return randStr
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
