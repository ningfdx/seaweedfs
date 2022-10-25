package command

import "testing"

func TestOptionCheckSum(t *testing.T) {
	mountPath := "/"

	quota := "10TiB"
	var inode uint64 = 600000

	readOnly := false

	t.Log(OptionCheckSum(mountPath, quota, inode, readOnly))
}
