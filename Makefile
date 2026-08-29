.PHONY: test npm-test cross-build npm-pack build clean

# Run all Go unit and race tests
test:
	go test -count=1 -race ./...

# Run Node.js wrapper automated test suite
npm-test:
	node --test test/npm_wrapper.test.js

# Cross-compile all platform binaries and generate checksums.txt
cross-build:
	bash scripts/build_npm_binaries.sh

# Verify npm package packing contents
npm-pack:
	npm pack --dry-run

# Build native binary locally
build:
	go build -trimpath -ldflags="-s -w" -o lightlimbs ./cmd/lightlimbs

# Clean build artifacts
clean:
	rm -rf dist/ lightlimbs weblimb weblimb.exe
