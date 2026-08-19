package main

import (
	"fmt"
	"azaffiliates/internal/auth"
)

func main() {
	t, err := auth.GenerateToken("local-test", "admin", "local-test")
	if err != nil { panic(err) }
	fmt.Println(t)
}
