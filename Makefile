BINARY = weed
ldflags="-X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltCommit=`git describe --all --long`'"
client-ldflags="-X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltType=client' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)' -X 'github.com/seaweedfs/seaweedfs/weed/util.BuiltCommit=`git describe --all --long`'"

SOURCE_DIR = .

all: install
client: client-install

client-install:
	cd weed; go install -ldflags ${client-ldflags}

install:
	cd weed; go install -ldflags ${ldflags}

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv"

tests:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv" -v ./...
