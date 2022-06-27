BINARY = weed
ldflags="-X 'github.com/chrislusf/seaweedfs/weed/util.BuiltTime=$(shell date +%Y-%m-%d/%H:%M:%S)'"

SOURCE_DIR = .

all: install

install:
	cd weed; go install -ldflags ${ldflags}

full_install:
	cd weed; go install -tags "elastic gocdk sqlite ydb tikv"

test:
	cd weed; go test -tags "elastic gocdk sqlite ydb tikv" -v ./...
