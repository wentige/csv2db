package main

func check(e error) {
	if e != nil {
		println(e)
	}
}

func panicIf(e error) {
	if e != nil {
		panic(e)
	}
}
