package mount

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/hanwen/go-fuse/v2/fuse"
	"math"
	"time"
)

const blockSize = 512

type statsCache struct {
	filer_pb.StatisticsResponse
	lastChecked int64 // unix time in seconds
}

func (wfs *WFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) (code fuse.Status) {

	glog.V(4).Infof("reading fs stats of %d", in.NodeId)

	rootDir := util.FullPath(wfs.option.FilerMountRootPath)
	if rootDir.IsQuotaRootNode() {
		cachedRootEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), rootDir)
		glog.V(4).Infof("reading fs stats of %s， cache is nil: %v", rootDir, cachedRootEntry == nil)
		if cacheErr == filer_pb.ErrNotFound {
			return fuse.OK
		}

		totalDiskSize := cachedRootEntry.GetXAttrSizeQuota()
		usedDiskSize := cachedRootEntry.GetXAttrSize()
		totalFileCount := cachedRootEntry.GetXAttrInodeQuota()
		actualFileCount := cachedRootEntry.GetXAttrInodeCount()

		glog.V(4).Infof("reading fs stats value: totalDiskSize %v ", totalDiskSize)
		glog.V(4).Infof("reading fs stats value: usedDiskSize %v ", usedDiskSize)
		glog.V(4).Infof("reading fs stats value: totalFileCount %v ", totalFileCount)
		glog.V(4).Infof("reading fs stats value: actualFileCount %v ", actualFileCount)

		// 超过上限了, 显示上限
		if usedDiskSize > totalDiskSize {
			totalDiskSize = usedDiskSize
		}
		if actualFileCount > totalFileCount {
			totalFileCount = actualFileCount
		}

		// Compute the total number of available blocks
		out.Blocks = totalDiskSize / blockSize

		// Compute the number of used blocks
		numBlocks := uint64(usedDiskSize / blockSize)

		// Report the number of free and available blocks for the block size
		out.Bfree = out.Blocks - numBlocks
		out.Bavail = out.Blocks - numBlocks
		out.Bsize = uint32(blockSize)

		// Report the total number of possible files in the file system (and those free)
		out.Files = totalFileCount
		out.Ffree = totalFileCount - actualFileCount

		// Report the maximum length of a name and the minimum fragment size
		out.NameLen = 1024
		out.Frsize = uint32(blockSize)

		return fuse.OK
	}

	if wfs.stats.lastChecked < time.Now().Unix()-20 {

		err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
				DiskType:    string(wfs.option.DiskType),
			}

			glog.V(4).Infof("reading filer stats: %+v", request)
			resp, err := client.Statistics(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("reading filer stats %v: %v", request, err)
				return err
			}
			glog.V(4).Infof("read filer stats: %+v", resp)

			wfs.stats.TotalSize = resp.TotalSize
			wfs.stats.UsedSize = resp.UsedSize
			wfs.stats.FileCount = resp.FileCount
			wfs.stats.lastChecked = time.Now().Unix()

			return nil
		})
		if err != nil {
			glog.V(0).Infof("filer Statistics: %v", err)
			return fuse.OK
		}
	}

	totalDiskSize := wfs.stats.TotalSize
	usedDiskSize := wfs.stats.UsedSize
	actualFileCount := wfs.stats.FileCount

	if wfs.option.Quota > 0 && totalDiskSize > uint64(wfs.option.Quota) {
		totalDiskSize = uint64(wfs.option.Quota)
		if usedDiskSize > totalDiskSize {
			totalDiskSize = usedDiskSize
		}
	}

	// Compute the total number of available blocks
	out.Blocks = totalDiskSize / blockSize

	// Compute the number of used blocks
	numBlocks := uint64(usedDiskSize / blockSize)

	// Report the number of free and available blocks for the block size
	out.Bfree = out.Blocks - numBlocks
	out.Bavail = out.Blocks - numBlocks
	out.Bsize = uint32(blockSize)

	// Report the total number of possible files in the file system (and those free)
	out.Files = math.MaxInt64
	out.Ffree = math.MaxInt64 - actualFileCount

	// Report the maximum length of a name and the minimum fragment size
	out.NameLen = 1024
	out.Frsize = uint32(blockSize)

	return fuse.OK
}
