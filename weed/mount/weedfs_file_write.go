package mount

import (
	"context"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"net/http"
	"syscall"
	"time"
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
		wfs.rootPathQuotaMapMutex.Lock()
		quotaInfo, ok := wfs.rootPathQuotaMap[rootDir.Name()]
		if !ok || quotaInfo.UpdatedAt.Add(time.Second*10).Before(time.Now()) {
			rootEntry, err := filer_pb.GetEntry(wfs, rootDir)
			if err != nil {
				glog.V(1).Infof("dir GetEntry %s: %v", rootDir, err)
				return 0, fuse.ENOENT
			}
			localEntry := filer.FromPbEntry(string(rootDir), rootEntry)
			quotaInfo = filer.QuotaInfo{
				QuotaSize:  localEntry.GetXAttrSizeQuota(),
				Size:       localEntry.GetXAttrSize(),
				QuotaInode: localEntry.GetXAttrInodeQuota(),
				Inode:      localEntry.GetXAttrInodeCount(),
				UpdatedAt:  time.Now(),
			}

			wfs.rootPathQuotaMap[rootDir.Name()] = quotaInfo
		}
		wfs.rootPathQuotaMapMutex.Unlock()

		wfs.writingPathMapMutex.Lock()
		writingSize := wfs.writingPathMap[string(fh.FullPath())] + uint64(len(data))
		wfs.writingPathMap[string(fh.FullPath())] = writingSize
		wfs.writingPathMapMutex.Unlock()

		glog.V(4).Infof("%v Write %d, quota %s: qs %d; s %d; qi %d; i %d, writing: %d", fh.FullPath(), len(data), rootDir, quotaInfo.QuotaSize, quotaInfo.Size, quotaInfo.QuotaInode, quotaInfo.Inode, writingSize)
		if quotaInfo.QuotaSize < quotaInfo.Size+writingSize {
			return 0, fuse.Status(syscall.EDQUOT)
		}
	}

	fh.dirtyPages.writerPattern.MonitorWriteAt(int64(in.Offset), int(in.Size))

	tsNs := time.Now().UnixNano()

	fh.Lock()
	defer fh.Unlock()

	entry := fh.GetEntry()
	if entry == nil {
		return 0, fuse.OK
	}

	entry.Content = nil
	offset := int64(in.Offset)
	entry.Attributes.FileSize = uint64(max(offset+int64(len(data)), int64(entry.Attributes.FileSize)))
	// glog.V(4).Infof("%v write [%d,%d) %d", fh.f.fullpath(), req.Offset, req.Offset+int64(len(req.Data)), len(req.Data))

	fh.dirtyPages.AddPage(offset, data, fh.dirtyPages.writerPattern.IsSequentialMode(), tsNs)

	written = uint32(len(data))

	if offset == 0 {
		// detect mime type
		fh.contentType = http.DetectContentType(data)
	}

	fh.dirtyMetadata = true

	if IsDebugFileReadWrite {
		// print("+")
		fh.mirrorFile.WriteAt(data, offset)
	}

	return written, fuse.OK
}
