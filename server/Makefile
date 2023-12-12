OBJPREFIX := "github.com/zilliztech/milvus-cdc/server/tag"
BUILD_TIME = $(shell date -u)
GIT_COMMIT = $(shell git rev-parse --short HEAD)
GO_VERSION = $(shell go version)
BUILD_FLAGS = "-X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'"

build:
	@echo "Building CDC ..."
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc main/main.go

build-position:
	@echo "Building Position ..."
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc-position tool/position/main.go

build-meta-position:
	@echo "Building Meta Position ..."
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc-meta-position tool/meta/main.go

build-generate-request:
	@echo "Building Generate Request ..."
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc-generate-request tool/generate_request/main.go