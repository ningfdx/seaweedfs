package command

import "testing"

func TestOptionCheckSum(t *testing.T) {
	mountPath := "/quota-1"

	quota := "10TiB"
	var inode uint64 = 600000

	readOnly := true

	t.Log(OptionCheckSum(mountPath, quota, inode, readOnly))
}
