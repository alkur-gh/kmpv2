
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
