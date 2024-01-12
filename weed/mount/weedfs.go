package mount

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/errors"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"golang.org/x/time/rate"
)

type Option struct {
	filerIndex         int32 // align memory for atomic read/write
	FilerAddresses     []pb.ServerAddress
	MountDirectory     string
	GrpcDialOption     grpc.DialOption
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	DiskType           types.DiskType
	ChunkSizeLimit     int64
	ConcurrentWriters  int
	ConcurrentReaders  int
	CacheDir           string
	CacheSizeMB        int64
	DataCenter         string
	Umask              os.FileMode
	Quota              int64
	DisableXAttr       bool
	ConcurrentLimit    int64
	AuthKey            string

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
	concurrentLimit   chan bool
	concurrentOpLimit *rate.Limiter
	writeLimiter      *rate.Limiter
	readLimiter       *rate.Limiter
	gcCh              chan struct{}

	rootPathQuotaMapMutex sync.Mutex
	rootPathQuotaMap      map[string]filer.QuotaInfo

	writingPathMapMutex sync.Mutex
	writingPathMap      map[string]uint64

	hostname string
	logfile  *os.File
}

func NewSeaweedFileSystem(option *Option) *WFS {
	concurrentLimit := option.ConcurrentLimit
	if concurrentLimit <= 1 {
		concurrentLimit = 2
	}
	opLimit := int(20 + concurrentLimit*10)
	acturalLimit := int(200 + (concurrentLimit * 10))
	wfs := &WFS{
		RawFileSystem:     fuse.NewDefaultRawFileSystem(),
		option:            option,
		signature:         util.RandomInt32(),
		inodeToPath:       NewInodeToPath(util.FullPath(option.FilerMountRootPath)),
		fhmap:             NewFileHandleToInode(),
		dhmap:             NewDirectoryHandleToInode(),
		concurrentLimit:   make(chan bool, concurrentLimit),
		concurrentOpLimit: rate.NewLimiter(rate.Limit(opLimit), int(opLimit)),
		writeLimiter:      util.NewLimiter(acturalLimit * util.MBpsLimit),
		readLimiter:       util.NewLimiter(acturalLimit * util.MBpsLimit),
		gcCh:              make(chan struct{}, 5),

		rootPathQuotaMap: make(map[string]filer.QuotaInfo),
		writingPathMap:   make(map[string]uint64),
	}
	var err error
	if wfs.hostname, err = os.Hostname(); err != nil {
		wfs.hostname = "unknown"
	}
	dellogs := make([]string, 0)
	var lastlogfile = ""
	lastlogtime := time.Now().Format("2006-01-02")
	logfiles, _ := os.ReadDir("/cache/")
	for _, logfile := range logfiles {
		if !strings.HasSuffix(logfile.Name(), "delete.log") {
			continue
		}
		re := regexp.MustCompile(`\d{4}-\d{2}-\d{2}`)
		matches := re.FindAllString(logfile.Name(), -1)
		if len(matches) == 0 {
			continue
		}
		dellogs = append(dellogs, matches[0])
		if strings.Compare(lastlogtime, matches[0]) == 1 {
			lastlogtime = matches[0]
			lastlogfile = logfile.Name()
		}
	}
	if len(dellogs) > 7 && strings.HasSuffix(lastlogfile, "delete.log") {
		os.Remove(fmt.Sprintf("/cache/%s", lastlogfile))
	}
	logName := fmt.Sprintf("/cache/%s-%s-delete.log", wfs.hostname, time.Now().Format("2006-01-02"))
	wfs.logfile, _ = os.OpenFile(logName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)

	wfs.option.filerIndex = int32(rand.Intn(len(option.FilerAddresses)))
	wfs.option.setupUniqueCacheDirectory()
	if option.CacheSizeMB > 0 {
		wfs.chunkCache = chunk_cache.NewTieredChunkCache(256, option.getUniqueCacheDir(), option.CacheSizeMB, 1024*1024)
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDir(), "meta"), option.UidGidMapper,
		util.FullPath(option.FilerMountRootPath),
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		}, func(path util.FullPath) bool {
			return wfs.inodeToPath.IsChildrenCached(path)
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
	//startTime := time.Now()
	//go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.option.FilerMountRootPath, startTime.UnixNano())
	go wfs.loopCheckQuota()
	go wfs.gcHelper()
	err := wfs.PrepareForQuota()
	if err != nil {
		glog.Fatalf("PrepareForQuota failed %s: %v", wfs.option.FilerMountRootPath, err)
		return
	}
	glog.Infof("PrepareForQuota finished %s", wfs.option.FilerMountRootPath)
}

func (wfs *WFS) PrepareForQuota() error {
	rootEntry, err := wfs.prepareRootCache()
	if err != nil {
		return errors.Wrap(err, "prepare root cache")
	}
	err = wfs.prepareQuota(rootEntry)
	if err != nil {
		return errors.Wrap(err, "prepare quota")
	}
	return nil
}

func (wfs *WFS) prepareRootCache() (*filer_pb.Entry, error) {
	filerPath := util.FullPath(wfs.option.FilerMountRootPath)
	glog.V(4).Infof("ReadRootEntries %s ...", filerPath)
	var rootEntry *filer_pb.Entry
	var getErr error

	err := util.Retry("ReadRootEntries", func() error {
		parent, _ := filerPath.DirAndName()
		if filerPath.IsRootNode() {
			rootEntry, getErr = filer_pb.GetEntry(wfs, filerPath)
			if errors.Is(getErr, filer_pb.ErrNotFound) {
				glog.V(4).Infof("No root path found, create it in %s  (%s)...", parent, filerPath)
				rootEntry = &filer_pb.Entry{
					Name:        filerPath.Name(),
					IsDirectory: true,
					Attributes: &filer_pb.FuseAttributes{
						FileSize: 4 * 1024,
						Mtime:    time.Now().Unix(),
						Crtime:   time.Now().Unix(),
						FileMode: uint32(os.ModeDir) | 0755,
					},
				}

				err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

					wfs.mapPbIdFromLocalToFiler(rootEntry)
					defer wfs.mapPbIdFromFilerToLocal(rootEntry)

					request := &filer_pb.CreateEntryRequest{
						Directory:  parent,
						Entry:      rootEntry,
						Signatures: []int32{wfs.signature},
					}

					glog.V(1).Infof("mkdir: %v", request)
					if err := filer_pb.CreateEntry(client, request); err != nil {
						glog.V(0).Infof("mkdir %s: %v", filerPath, err)
						return err
					}

					if err := wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
						return fmt.Errorf("local mkdir dir %s: %v", filerPath, err)
					}

					return nil
				})
				if err != nil {
					return errors.Wrap(err, "filer client create entry failed")
				}
			} else if getErr != nil {
				return getErr
			}
		} else {
			rootEntry = &filer_pb.Entry{
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					FileSize: 4 * 1024,
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(os.ModeDir) | 0755,
				},
				Extended: rootEntry.Extended,
			}
		}

		entry := filer.FromPbEntry("/", rootEntry)
		if err := wfs.metaCache.InsertEntry(context.Background(), entry); err != nil {
			glog.V(0).Infof("read %s: %v", entry.FullPath, err)
			return err
		}
		return nil
	})
	glog.V(4).Infof("ReadRootEntries %s res: %v ...", filerPath, err)
	return rootEntry, err
}

func (wfs *WFS) prepareQuota(entry *filer_pb.Entry) error {
	filerPath := util.FullPath(wfs.option.FilerMountRootPath)
	if !filerPath.IsQuotaRootNode() {
		return nil
	}
	localEntry := filer.FromPbEntry("/", entry)

	b, err := util.ParseBytes(wfs.option.DirectoryQuotaSize)
	if err != nil {
		glog.Errorf("ParseBytes DirectoryQuotaSize %s: %v", wfs.option.DirectoryQuotaSize, err)
		return err
	}

	localEntry.SetXAttrSizeQuota(int64(b))
	localEntry.SetXAttrInodeCountQuota(int64(wfs.option.DirectoryQuotaInode))
	code := wfs.saveEntry(localEntry.FullPath, localEntry.ToProtoEntry())
	if code != fuse.OK {
		glog.Errorf("set DirectoryQuota failed %s", wfs.option.FilerMountRootPath)
		return errors.New("wfs save entry failed")
	}
	glog.Errorf("set DirectoryQuota success %s, %d", wfs.option.DirectoryQuotaSize, wfs.option.DirectoryQuotaInode)

	return nil
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
		entry = fh.UpdateEntry(func(entry *filer_pb.Entry) {
			if entry != nil && fh.entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
		})
	} else {
		entry, status = wfs.maybeLoadEntry(path)
	}
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
	i := atomic.LoadInt32(&wfs.option.filerIndex)
	return wfs.option.FilerAddresses[i]
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
