package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/mount/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/mount_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/chunk_cache"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
)

type Option struct {
	MountDirectory     string
	FilerAddresses     []pb.ServerAddress
	filerIndex         int
	GrpcDialOption     grpc.DialOption
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	DiskType           types.DiskType
	ChunkSizeLimit     int64
	ConcurrentWriters  int
	CacheDir           string
	CacheSizeMB        int64
	DataCenter         string
	Umask              os.FileMode
	Quota              int64
	DisableXAttr       bool

	// if mount point is in quota-* format, then you can use these option
	DirectoryQuotaSize  string
	DirectoryQuotaInode uint64

	MountUid         uint32
	MountGid         uint32
	MountMode        os.FileMode
	MountCtime       time.Time
	MountMtime       time.Time
	MountParentInode uint64

	VolumeServerAccess string // how to access volume servers
	Cipher             bool   // whether encrypt data on volume server
	UidGidMapper       *meta_cache.UidGidMapper

	uniqueCacheDir         string
	uniqueCacheTempPageDir string
}

type WFS struct {
	// https://dl.acm.org/doi/fullHtml/10.1145/3310148
	// follow https://github.com/hanwen/go-fuse/blob/master/fuse/api.go
	fuse.RawFileSystem
	mount_pb.UnimplementedSeaweedMountServer
	fs.Inode
	option            *Option
	metaCache         *meta_cache.MetaCache
	stats             statsCache
	chunkCache        *chunk_cache.TieredChunkCache
	signature         int32
	concurrentWriters *util.LimitedConcurrentExecutor
	inodeToPath       *InodeToPath
	fhmap             *FileHandleToInode
	dhmap             *DirectoryHandleToInode
	fuseServer        *fuse.Server
	IsOverQuota       bool
}

func NewSeaweedFileSystem(option *Option) *WFS {
	wfs := &WFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		option:        option,
		signature:     util.RandomInt32(),
		inodeToPath:   NewInodeToPath(util.FullPath(option.FilerMountRootPath)),
		fhmap:         NewFileHandleToInode(),
		dhmap:         NewDirectoryHandleToInode(),
	}

	wfs.option.filerIndex = rand.Intn(len(option.FilerAddresses))
	wfs.option.setupUniqueCacheDirectory()
	if option.CacheSizeMB > 0 {
		wfs.chunkCache = chunk_cache.NewTieredChunkCache(256, option.getUniqueCacheDir(), option.CacheSizeMB, 1024*1024)
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDir(), "meta"), option.UidGidMapper,
		util.FullPath(option.FilerMountRootPath),
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		}, func(path util.FullPath) bool {
			// 需要缓存根目录，因为根目录的更新再quota逻辑中也同样重要
			return wfs.inodeToPath.IsChildrenCached(path) || path == "/"
		}, func(filePath util.FullPath, entry *filer_pb.Entry) {
		})
	grace.OnInterrupt(func() {
		wfs.metaCache.Shutdown()
		os.RemoveAll(option.getUniqueCacheDir())
	})

	if wfs.option.ConcurrentWriters > 0 {
		wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(wfs.option.ConcurrentWriters)
	}
	return wfs
}

func (wfs *WFS) StartBackgroundTasks() {
	startTime := time.Now()
	go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.option.FilerMountRootPath, startTime.UnixNano())
	go wfs.loopCheckQuota()
	err := meta_cache.RootCache(wfs.metaCache, wfs, util.FullPath(wfs.option.FilerMountRootPath))
	if err != nil {
		glog.Errorf("meta_cache.RootCache failed %s: %v", wfs.option.FilerMountRootPath, err)
		return
	}
	glog.Infof("meta_cache.RootCache finished %s", wfs.option.FilerMountRootPath)

	go func() {
		time.Sleep(time.Millisecond * 400)
		if util.FullPath(wfs.option.FilerMountRootPath).IsRootNode() {
			glog.Infof("mount root path is a quotaNode")
			entry, err := filer_pb.GetEntry(wfs, util.FullPath(wfs.option.FilerMountRootPath))
			if err != nil {
				glog.Errorf("dir GetEntry %s: %v", wfs.option.FilerMountRootPath, err)
				return
			}
			localEntry := filer.FromPbEntry("/", entry)

			b, err := util.ParseBytes(wfs.option.DirectoryQuotaSize)
			if err != nil {
				glog.Errorf("ParseBytes DirectoryQuotaSize %s: %v", wfs.option.DirectoryQuotaSize, err)
				return
			}

			localEntry.SetXAttrSizeQuota(int64(b))
			localEntry.SetXAttrInodeCountQuota(int64(wfs.option.DirectoryQuotaInode))
			code := wfs.saveEntry(localEntry.FullPath, localEntry.ToProtoEntry())
			if code != fuse.OK {
				glog.Errorf("set DirectoryQuota failed %s", wfs.option.FilerMountRootPath)
				return
			}
			glog.Errorf("set DirectoryQuota success %s, %d", wfs.option.DirectoryQuotaSize, wfs.option.DirectoryQuotaInode)
		}

	}()
}

func (wfs *WFS) String() string {
	return "seaweedfs"
}

func (wfs *WFS) Init(server *fuse.Server) {
	wfs.fuseServer = server
}

func (wfs *WFS) maybeReadEntry(inode uint64) (path util.FullPath, fh *FileHandle, entry *filer_pb.Entry, status fuse.Status) {
	path, status = wfs.inodeToPath.GetPath(inode)
	if status != fuse.OK {
		return
	}
	var found bool
	if fh, found = wfs.fhmap.FindFileHandle(inode); found {
		entry = fh.GetEntry()
		if entry != nil && fh.entry.Attributes == nil {
			entry.Attributes = &filer_pb.FuseAttributes{}
		}
		return path, fh, entry, fuse.OK
	}
	entry, status = wfs.maybeLoadEntry(path)
	return
}

func (wfs *WFS) maybeLoadEntry(fullpath util.FullPath) (*filer_pb.Entry, fuse.Status) {

	// glog.V(3).Infof("read entry cache miss %s", fullpath)
	dir, name := fullpath.DirAndName()

	// return a valid entry for the mount root
	if string(fullpath) == wfs.option.FilerMountRootPath {
		return &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    wfs.option.MountMtime.Unix(),
				FileMode: uint32(wfs.option.MountMode),
				Uid:      wfs.option.MountUid,
				Gid:      wfs.option.MountGid,
				Crtime:   wfs.option.MountCtime.Unix(),
			},
		}, fuse.OK
	}

	// read from async meta cache
	meta_cache.EnsureVisited(wfs.metaCache, wfs, util.FullPath(dir))
	cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
	if cacheErr == filer_pb.ErrNotFound {
		return nil, fuse.ENOENT
	}
	return cachedEntry.ToProtoEntry(), fuse.OK
}

func (wfs *WFS) LookupFn() wdclient.LookupFileIdFunctionType {
	if wfs.option.VolumeServerAccess == "filerProxy" {
		return func(fileId string) (targetUrls []string, err error) {
			return []string{"http://" + wfs.getCurrentFiler().ToHttpAddress() + "/?proxyChunkId=" + fileId}, nil
		}
	}
	return filer.LookupFn(wfs)
}

func (wfs *WFS) getCurrentFiler() pb.ServerAddress {
	return wfs.option.FilerAddresses[wfs.option.filerIndex]
}

func (option *Option) setupUniqueCacheDirectory() {
	cacheUniqueId := util.Md5String([]byte(option.MountDirectory + string(option.FilerAddresses[0]) + option.FilerMountRootPath + util.Version()))[0:8]
	option.uniqueCacheDir = path.Join(option.CacheDir, cacheUniqueId)
	option.uniqueCacheTempPageDir = filepath.Join(option.uniqueCacheDir, "swap")
	os.MkdirAll(option.uniqueCacheTempPageDir, os.FileMode(0777)&^option.Umask)
}

func (option *Option) getTempFilePageDir() string {
	return option.uniqueCacheTempPageDir
}

func (option *Option) getUniqueCacheDir() string {
	return option.uniqueCacheDir
}
