package kafka

func check(err error) {
	if err != nil {
		panic(err)
	}
}
