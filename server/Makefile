OBJPREFIX := "github.com/zilliztech/milvus-cdc/server/tag"
BUILD_TIME = $(shell date -u)
GIT_COMMIT ?= $(if $(CDC_GIT_COMMIT),$(CDC_GIT_COMMIT),$(shell git rev-parse --short HEAD))
GO_VERSION = $(shell go version)
BUILD_FLAGS = "-X '$(OBJPREFIX).BuildTime=$(BUILD_TIME)' -X '$(OBJPREFIX).GitCommit=$(GIT_COMMIT)' -X '$(OBJPREFIX).GoVersion=$(GO_VERSION)'"

build:
	@echo "Building CDC ..."
	@echo "Build Time: $(BUILD_TIME)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Go Version: $(GO_VERSION)"
	@go build -ldflags $(BUILD_FLAGS) -o ../bin/cdc main/main.go