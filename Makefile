GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2

.PHONY: format lint lint-fix lint-config test

format:
	$(GOLANGCI_LINT) fmt ./...

lint:
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run --fix ./...

lint-config:
	$(GOLANGCI_LINT) config verify

test:
	go test ./...
