package shell

import (
	"encoding/json"
	"flag"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/erasure_coding"
	"golang.org/x/exp/slices"
	"path/filepath"

	"io"
)

func init() {
	Commands = append(Commands, &commandAutoDLVolumeList{})
}

type commandAutoDLVolumeList struct {
	collectionPattern *string
	readonly          *bool
	volumeId          *uint64
}

func (c *commandAutoDLVolumeList) Name() string {
	return "autodl.volume.list"
}

func (c *commandAutoDLVolumeList) Help() string {
	return `list all volumes

	This command list all volumes as a tree of dataCenter > rack > dataNode > volume.

`
}

func (c *commandAutoDLVolumeList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeListCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	c.collectionPattern = volumeListCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	c.readonly = volumeListCommand.Bool("readonly", false, "show only readonly")
	c.volumeId = volumeListCommand.Uint64("volumeId", 0, "show only volume id")

	if err = volumeListCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	topologyInfo, _, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	output, err := json.Marshal(topologyInfo)
	if err != nil {
		return err
	}
	_, err = writer.Write(output)
	return
}

func (c *commandAutoDLVolumeList) writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo, volumeSizeLimitMb uint64, verbosityLevel int) statistics {
	output(verbosityLevel >= 0, writer, "Topology volumeSizeLimit:%d MB%s\n", volumeSizeLimitMb, diskInfosToString(t.DiskInfos))
	slices.SortFunc(t.DataCenterInfos, func(a, b *master_pb.DataCenterInfo) bool {
		return a.Id < b.Id
	})
	var s statistics
	for _, dc := range t.DataCenterInfos {
		s = s.plus(c.writeDataCenterInfo(writer, dc, verbosityLevel))
	}
	output(verbosityLevel >= 0, writer, "%+v \n", s)
	return s
}

func (c *commandAutoDLVolumeList) writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 1, writer, "  DataCenter %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	slices.SortFunc(t.RackInfos, func(a, b *master_pb.RackInfo) bool {
		return a.Id < b.Id
	})
	for _, r := range t.RackInfos {
		s = s.plus(c.writeRackInfo(writer, r, verbosityLevel))
	}
	output(verbosityLevel >= 1, writer, "  DataCenter %s %+v \n", t.Id, s)
	return s
}

func (c *commandAutoDLVolumeList) writeRackInfo(writer io.Writer, t *master_pb.RackInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 2, writer, "    Rack %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	slices.SortFunc(t.DataNodeInfos, func(a, b *master_pb.DataNodeInfo) bool {
		return a.Id < b.Id
	})
	for _, dn := range t.DataNodeInfos {
		s = s.plus(c.writeDataNodeInfo(writer, dn, verbosityLevel))
	}
	output(verbosityLevel >= 2, writer, "    Rack %s %+v \n", t.Id, s)
	return s
}

func (c *commandAutoDLVolumeList) writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo, verbosityLevel int) statistics {
	output(verbosityLevel >= 3, writer, "      DataNode %s%s\n", t.Id, diskInfosToString(t.DiskInfos))
	var s statistics
	for _, diskInfo := range t.DiskInfos {
		s = s.plus(c.writeDiskInfo(writer, diskInfo, verbosityLevel))
	}
	output(verbosityLevel >= 3, writer, "      DataNode %s %+v \n", t.Id, s)
	return s
}

func (c *commandAutoDLVolumeList) isNotMatchDiskInfo(readOnly bool, collection string, volumeId uint32) bool {
	if *c.readonly && !readOnly {
		return true
	}
	if *c.collectionPattern != "" {
		if matched, _ := filepath.Match(*c.collectionPattern, collection); !matched {
			return true
		}
	}
	if *c.volumeId > 0 && *c.volumeId != uint64(volumeId) {
		return true
	}
	return false
}

func (c *commandAutoDLVolumeList) writeDiskInfo(writer io.Writer, t *master_pb.DiskInfo, verbosityLevel int) statistics {
	var s statistics
	diskType := t.Type
	if diskType == "" {
		diskType = "hdd"
	}
	output(verbosityLevel >= 4, writer, "        Disk %s(%s)\n", diskType, diskInfoToString(t))
	slices.SortFunc(t.VolumeInfos, func(a, b *master_pb.VolumeInformationMessage) bool {
		return a.Id < b.Id
	})
	for _, vi := range t.VolumeInfos {
		if c.isNotMatchDiskInfo(vi.ReadOnly, vi.Collection, vi.Id) {
			continue
		}
		s = s.plus(writeVolumeInformationMessage(writer, vi, verbosityLevel))
	}
	for _, ecShardInfo := range t.EcShardInfos {
		if c.isNotMatchDiskInfo(false, ecShardInfo.Collection, ecShardInfo.Id) {
			continue
		}
		output(verbosityLevel >= 5, writer, "          ec volume id:%v collection:%v shards:%v\n", ecShardInfo.Id, ecShardInfo.Collection, erasure_coding.ShardBits(ecShardInfo.EcIndexBits).ShardIds())
	}
	output(verbosityLevel >= 4, writer, "        Disk %s %+v \n", diskType, s)
	return s
}
