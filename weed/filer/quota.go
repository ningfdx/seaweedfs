package filer

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"path/filepath"
	"strings"
	"time"
)

type event struct {
	fullPath          string
	directory         string
	eventNotification *filer_pb.EventNotification
}

func (f *Filer) eventHandler() chan<- *event {
	newCh := make(chan *event, 3000)

	handleCh := make(chan map[string]int64, 5)
	go f.runEventHandler(newCh, handleCh)
	go f.handleQuotaPersist(handleCh)

	return newCh
}

func (f *Filer) runEventHandler(ch <-chan *event, persistCh chan map[string]int64) {
	// 只统计 collection 根目录下的directory节点，避免递归调用，锁竞争导致效率低下
	rootNodeMapping := make(map[string]int64)
	var sizeChanged int64

	ticker := time.NewTicker(time.Second * 15)

	for {
		select {
		case <-ticker.C:
			copyTmp := rootNodeMapping
			rootNodeMapping = make(map[string]int64)
			persistCh <- copyTmp
		case msg, ok := <-ch:
			if !ok {
				return
			}
			tp := typeParse(msg)
			glog.V(4).Infof("event handler got %s type of %s", tp, msg.fullPath)
			switch tp {
			case updateOp:
				sizeChanged = int64(msg.eventNotification.NewEntry.Attributes.GetFileSize()) - int64(msg.eventNotification.OldEntry.Attributes.GetFileSize())
			case deleteOp:
				sizeChanged = -int64(msg.eventNotification.OldEntry.Attributes.GetFileSize())
			default:
				sizeChanged = 0
			}
			if sizeChanged != 0 {
				splits := strings.Split(strings.TrimLeft(msg.fullPath, string(filepath.Separator)), string(filepath.Separator))
				if len(splits) == 0 {
					continue
				}
				rootNode := string(filepath.Separator) + splits[0]
				rootNodeMapping[rootNode] = rootNodeMapping[rootNode] + sizeChanged
				glog.V(4).Infof("%s type of %s, rootNode: %s, splits: %v, %d", tp, msg.fullPath, rootNode, splits, sizeChanged)
				sizeChanged = 0
			}
		}
	}
}

func (f *Filer) handleQuotaPersist(persistCh chan map[string]int64) {
	for changes := range persistCh {
		for node, sizeChanged := range changes {
			if sizeChanged == 0 {
				continue
			}
			entry, err := f.FindEntry(context.Background(), util.FullPath(node))
			if err != nil {
				glog.Errorf("find entry of %s failed: %s", node, err.Error())
				continue
			}
			glog.V(4).Infof("fin entry of %s, used_size: %d", node, entry.GetXAttrSize())

			usedSize := uint64(int64(entry.GetXAttrSize()) + sizeChanged)
			entry.SetXAttrSize(usedSize)

			err = f.UpdateEntry(context.Background(), nil, entry)
			if err != nil {
				glog.Errorf("update entry of %s failed: %s", node, err.Error())
				continue
			}
			f.NotifyUpdateEvent(context.Background(), entry, entry, false, false, []int32{util.RandomInt32()})

			glog.V(4).Infof("handleQuotaPersist of %s changed: %d, used_size: %d", node, sizeChanged, usedSize)
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
