package kafka

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
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

func getPartitions(c sarama.Consumer, topic, partitions string) ([]int32, error) {
	if partitions == "all" || partitions == "" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
