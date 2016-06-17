package kafka

/*
NOTE: Current Consumer implements a wrapper to ConsumerGroups (should be refactored asap)
*/

// Consumer Consume messages, retieves when first message arrives
func Consumer(reqTopic string) (string, []byte) {
	key, value := ConsumerGroups(reqTopic)
	return key, value
}

// ConsumerMarkOffset Marks message offset after being sucessfully processed
func ConsumerMarkOffset() {
	ConsumerGroupsMarkOffset()
}

// ConsumerClose function to Close Consumer
func ConsumerClose() {
	ConsumerGroupsClose()
}
