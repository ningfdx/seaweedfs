GOPROXY=export GO111MODULE=on && export GOPRIVATE=gitlab-gpuhub.autodl.com && export GOPROXY=https://goproxy.cn
GOCMD=$(GOPROXY) && go
GORUN=$(GOCMD) run
DOCKER=docker
BINARY = weed
ldflags="-X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltCommit=`git describe --all --long`'"

SOURCE_DIR = .

all: install

install:
	cd weed; go install

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv"

tests:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv" -v ./...

server_build:
	cd weed; go install -ldflags ${ldflags}

build:
	cd weed; $(GOCMD) build -o weed -ldflags ${ldflags}

docker_build:
	$(DOCKER) run --rm -w /code/ -v /tmp/go_mod:/root/go/pkg/mod -v $(shell pwd):/code ningfd/golang:1.20-ubuntu20.04 sh -c "make build"
