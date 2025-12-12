tests:
	go test -cover ./test -v

samples:
	go run ./cmd/sample/main.go
