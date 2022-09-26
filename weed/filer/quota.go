package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"path/filepath"
	"time"
)

type event struct {
	fullPath          util.FullPath
	directory         string
	eventNotification *filer_pb.EventNotification
}

func (f *Filer) eventHandler() chan<- *event {
	newCh := make(chan *event, 3000)

	handleCh := make(chan map[util.FullPath]rootNodeAttr, 5)
	go f.runEventHandler(newCh, handleCh)
	go f.handleQuotaPersist(handleCh)

	return newCh
}

type rootNodeAttr struct {
	sizeChanged       int64
	inodeCountChanged int64
}

func (f *Filer) runEventHandler(ch <-chan *event, persistCh chan map[util.FullPath]rootNodeAttr) {
	// 只统计 collection 根目录下的directory节点，避免递归调用，锁竞争导致效率低下
	rootNodeMapping := make(map[util.FullPath]rootNodeAttr)
	var sizeChanged int64
	var inodeCountChanged int64

	ticker := time.NewTicker(time.Second * 3)

	for {
		select {
		case <-ticker.C:
			copyTmp := rootNodeMapping
			rootNodeMapping = make(map[util.FullPath]rootNodeAttr)
			persistCh <- copyTmp
		case msg, ok := <-ch:
			if !ok {
				return
			}
			tp := typeParse(msg)
			glog.V(4).Infof("event handler got %s type of %s", tp, msg.fullPath)
			switch tp {
			case createOp:
				inodeCountChanged += 1
				sizeChanged = int64(msg.eventNotification.NewEntry.Attributes.GetFileSize())
			case updateOp:
				sizeChanged = int64(msg.eventNotification.NewEntry.Attributes.GetFileSize()) - int64(msg.eventNotification.OldEntry.Attributes.GetFileSize())
			case deleteOp:
				inodeCountChanged -= 1
				sizeChanged = -int64(msg.eventNotification.OldEntry.Attributes.GetFileSize())
			default:
				sizeChanged = 0
				inodeCountChanged = 0
			}
			if sizeChanged != 0 || inodeCountChanged != 0 {
				exist, rootNode := msg.fullPath.GetRootDir()
				if !exist {
					continue
				}
				if rootNode == util.FullPath(filepath.Separator) {
					continue
				}
				rootNodeMapping[rootNode] = rootNodeAttr{
					sizeChanged:       rootNodeMapping[rootNode].sizeChanged + sizeChanged,
					inodeCountChanged: rootNodeMapping[rootNode].inodeCountChanged + inodeCountChanged,
				}
				glog.V(4).Infof("%s type of %s, rootNode: %s, %d - %d", tp, msg.fullPath, rootNode, sizeChanged, inodeCountChanged)
				sizeChanged = 0
				inodeCountChanged = 0
			}
		}
	}
}

func (f *Filer) handleQuotaPersist(persistCh chan map[util.FullPath]rootNodeAttr) {
	for changes := range persistCh {
		for node, val := range changes {
			if node == util.FullPath(filepath.Separator) {
				continue
			}

			if val.sizeChanged == 0 && val.inodeCountChanged == 0 {
				continue
			}

			usedSize, err := f.quotaPlugin.SizeIncrement(node.Name(), val.sizeChanged)
			if err != nil {
				glog.Errorf("update entry of %s failed: %s", node, err.Error())
				continue
			}
			usedInode, err := f.quotaPlugin.InodeIncrement(node.Name(), val.inodeCountChanged)
			if err != nil {
				glog.Errorf("update entry of %s failed: %s", node, err.Error())
				continue
			}

			glog.V(4).Infof("handleQuotaPersist of %s changed: size %d, inode %d, used_size: %d, used_inode: %d", node, val.sizeChanged, val.inodeCountChanged, usedSize, usedInode)
		}
	}
}

type eventType string

const (
	createOp eventType = "create"
	updateOp eventType = "update"
	deleteOp eventType = "delete"
	renameOp eventType = "rename"
)

/*
https://github.com/chrislusf/seaweedfs/wiki/Filer-Change-Data-Capture

Type	Directory	NewEntry	OldEntry	NewParentPath
Create	exists	exists	null	equal to Directory
Update	exists	exists	exists	equal to Directory
Delete	exists	null	exists	equal to Directory
Rename	exists	exists	exists	not equal to Directory
*/
func typeParse(ev *event) eventType {
	// Create
	if ev.eventNotification.NewEntry != nil &&
		ev.eventNotification.OldEntry == nil &&
		ev.directory == ev.eventNotification.NewParentPath {
		return createOp
	}

	// Update
	if ev.eventNotification.NewEntry != nil &&
		ev.eventNotification.OldEntry != nil &&
		ev.directory == ev.eventNotification.NewParentPath {
		return updateOp
	}

	// Delete
	if ev.eventNotification.NewEntry == nil &&
		ev.eventNotification.OldEntry != nil {
		return deleteOp
	}

	// Rename
	if ev.eventNotification.NewEntry != nil &&
		ev.eventNotification.OldEntry != nil &&
		ev.directory != ev.eventNotification.NewParentPath {
		return renameOp
	}

	return ""
}
