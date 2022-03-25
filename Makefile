
.PHONY: run
run: web


.PHONY: web
web:
	@goimports -w .
	@go run cmd/http/main.go

.PHONY: test
test:
	@goimports -w .
	@go vet ./...
	@go test ./... -race

.PHONY: compilepb
compilepb: api/v1/*.proto
	@protoc api/v1/*.proto \
		--proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative
