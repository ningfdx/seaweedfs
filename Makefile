BINARY = weed
ldflags="-X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltCommit=`git describe --all --long`'"
client-ldflags="-X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltType=client' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltCommit=`git describe --all --long`'"

SOURCE_DIR = .

all: build
client: client-install

client-install:
	cd weed; go install -ldflags ${client-ldflags}

install:
	cd weed; go install -ldflags ${ldflags}

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv"

tests:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv" -v ./...

build:
	docker run --rm -w /code/ -v /tmp/go_mod:/root/go/pkg/mod -v /home/nfd/nfd/poyehali/seaweedfs/:/code ningfd/golang:1.9-ubuntu20.04 sh -c "make do_build"

do_build:
	cd weed; go build -o weed -ldflags ${ldflags} .