package mount

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"net/http"
	"syscall"
)

/**
 * Write data
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the file has
 * been opened in 'direct_io' mode, in which case the return value
 * of the write system call will reflect the return value of this
 * operation.
 *
 * Unless FUSE_CAP_HANDLE_KILLPRIV is disabled, this method is
 * expected to reset the setuid and setgid bits.
 *
 * fi->fh will contain the value set by the open method, or will
 * be undefined if the open method didn't set any value.
 *
 * Valid replies:
 *   fuse_reply_write
 *   fuse_reply_err
 *
 * @param req request handle
 * @param ino the inode number
 * @param buf data to write
 * @param size number of bytes to write
 * @param off offset to write to
 * @param fi file information
 */
func (wfs *WFS) Write(cancel <-chan struct{}, in *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	wfs.concurrentLimit <- true
	defer func() {
		<-wfs.concurrentLimit
	}()
	if err := wfs.writeLimiter.WaitN(context.Background(), len(data)); err != nil {
		return 0, fuse.Status(syscall.EUSERS)
	}

	if wfs.IsOverQuota {
		return 0, fuse.Status(syscall.ENOSPC)
	}

	fh := wfs.GetHandle(FileHandleId(in.Fh))
	if fh == nil {
		return 0, fuse.ENOENT
	}

	exist, rootDir := fh.FullPath().GetRootDir()
	if exist {
		cachedRootEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), rootDir)
		if cacheErr == filer_pb.ErrNotFound {
			return 0, fuse.ENOENT
		}
		quotaSize := cachedRootEntry.GetXAttrSizeQuota()
		usedSize := cachedRootEntry.GetXAttrSize()
		glog.V(4).Infof("%v Write, quota %s: %s/%s", fh.FullPath(), rootDir, cachedRootEntry.Extended[XATTR_PREFIX+filer.Size_Key], cachedRootEntry.Extended[XATTR_PREFIX+filer.Size_Quota_Key])
		glog.V(4).Infof("%v Write, quota %s: %d/%d", fh.FullPath(), rootDir, usedSize, quotaSize)
		if quotaSize < uint64(len(data))+usedSize {
			return 0, fuse.Status(syscall.EDQUOT)
		}

		cachedRootEntry.SetXAttrSize(int64(usedSize + uint64(len(data))))
		wfs.metaCache.UpdateEntry(context.Background(), cachedRootEntry)
	}

	fh.dirtyPages.writerPattern.MonitorWriteAt(int64(in.Offset), int(in.Size))

	fh.orderedMutex.Acquire(context.Background(), 1)
	defer fh.orderedMutex.Release(1)

	entry := fh.entry
	if entry == nil {
		return 0, fuse.OK
	}

	entry.Content = nil
	offset := int64(in.Offset)
	entry.Attributes.FileSize = uint64(max(offset+int64(len(data)), int64(entry.Attributes.FileSize)))
	// glog.V(4).Infof("%v write [%d,%d) %d", fh.f.fullpath(), req.Offset, req.Offset+int64(len(req.Data)), len(req.Data))

	fh.dirtyPages.AddPage(offset, data, fh.dirtyPages.writerPattern.IsSequentialMode())

	written = uint32(len(data))

	if offset == 0 {
		// detect mime type
		fh.contentType = http.DetectContentType(data)
	}

	fh.dirtyMetadata = true

	return written, fuse.OK
}
