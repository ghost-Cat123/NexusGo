package main

import (
	"fmt"
	"os"
)

func main() {
	const hash = "$2a$10$VphIAAOpHpsEXIrx0OYsm.meJ3lhbiD3PbR6T25nRqIwmNNXS21Uq"
	const start, end = 1401, 2500

	f, _ := os.Create("insert_users.sql")
	defer f.Close()

	fmt.Fprintf(f, "INSERT INTO users (user_id, username, password, nickname, avatar) VALUES\n")

	for id := start; id <= end; id++ {
		comma := ""
		if id < end {
			comma = ","
		}
		fmt.Fprintf(f,
			"(%d, 'test_user_%d', '%s', 'test_user_%d', '')%s\n",
			id, id, hash, id, comma)
	}
	fmt.Fprintf(f, "ON DUPLICATE KEY UPDATE username=VALUES(username);\n")
	fmt.Printf("Generated insert_users.sql with %d rows\n", end-start+1)
}
