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
