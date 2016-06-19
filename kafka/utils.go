package kafka

import (
	"fmt"
	"math/rand"
	"os"
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

func printError(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "Producer: ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
}
