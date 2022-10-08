package shell

import (
	"flag"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"golang.org/x/exp/slices"
	"io"
	"path/filepath"
)

var (
	GatherShell = prometheus.NewRegistry()

	VolumeServerDeleteSizeGaugeShell = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "SeaweedFS",
			Subsystem: "volumeServer",
			Name:      "volume_delete_size",
			Help:      "Actual delete size by volumes.",
		}, []string{"dataNodeIdVolumeId"})
)

func init() {
	Commands = append(Commands, &commandVolumeDeleteList{})
	GatherShell.MustRegister(VolumeServerDeleteSizeGaugeShell)
}

type commandVolumeDeleteList struct {
	collectionPattern *string
	readonly          *bool
	volumeId          *uint64
}

func (c *commandVolumeDeleteList) Name() string {
	return "volume.delete.count"
}

func (c *commandVolumeDeleteList) Help() string {
	return `This command list deleteBytes for all volumes.
`
}

func (c *commandVolumeDeleteList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) (err error) {

	volumeDeleteListCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	verbosityLevel := volumeDeleteListCommand.Int("v", 5, "verbose mode: 0, 1, 2, 3, 4, 5")
	c.collectionPattern = volumeDeleteListCommand.String("collectionPattern", "", "match with wildcard characters '*' and '?'")
	c.readonly = volumeDeleteListCommand.Bool("readonly", false, "show only readonly")
	c.volumeId = volumeDeleteListCommand.Uint64("volumeId", 0, "show only volume id")

	if err = volumeDeleteListCommand.Parse(args); err != nil {
		return nil
	}

	// collect topology information
	topologyInfo, volumeSizeLimitMb, err := collectTopologyInfo(commandEnv, 0)
	if err != nil {
		return err
	}

	c.writeTopologyInfo(writer, topologyInfo, volumeSizeLimitMb, *verbosityLevel)
	return nil
}

func (c *commandVolumeDeleteList) writeTopologyInfo(writer io.Writer, t *master_pb.TopologyInfo, volumeSizeLimitMb uint64, verbosityLevel int) {
	slices.SortFunc(t.DataCenterInfos, func(a, b *master_pb.DataCenterInfo) bool {
		return a.Id < b.Id
	})
	for _, dc := range t.DataCenterInfos {
		c.writeDataCenterInfo(writer, dc, verbosityLevel)
	}
	return
}

func (c *commandVolumeDeleteList) writeDataCenterInfo(writer io.Writer, t *master_pb.DataCenterInfo, verbosityLevel int) {
	slices.SortFunc(t.RackInfos, func(a, b *master_pb.RackInfo) bool {
		return a.Id < b.Id
	})
	for _, r := range t.RackInfos {
		c.writeRackInfo(writer, r, verbosityLevel)
	}
	return
}

func (c *commandVolumeDeleteList) writeRackInfo(writer io.Writer, t *master_pb.RackInfo, verbosityLevel int) {
	slices.SortFunc(t.DataNodeInfos, func(a, b *master_pb.DataNodeInfo) bool {
		return a.Id < b.Id
	})
	for _, dn := range t.DataNodeInfos {
		c.writeDataNodeInfo(writer, dn, verbosityLevel)
	}
	return
}

func (c *commandVolumeDeleteList) writeDataNodeInfo(writer io.Writer, t *master_pb.DataNodeInfo, verbosityLevel int) {
	for _, diskInfo := range t.DiskInfos {
		c.writeDiskInfo(writer, diskInfo, verbosityLevel, t.Id)
	}
	return
}

func (c *commandVolumeDeleteList) isNotMatchDiskInfo(readOnly bool, collection string, volumeId uint32) bool {
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

func (c *commandVolumeDeleteList) writeDiskInfo(writer io.Writer, t *master_pb.DiskInfo, verbosityLevel int, dataNodeId string) {
	slices.SortFunc(t.VolumeInfos, func(a, b *master_pb.VolumeInformationMessage) bool {
		return a.Id < b.Id
	})
	for _, vi := range t.VolumeInfos {
		if c.isNotMatchDiskInfo(vi.ReadOnly, vi.Collection, vi.Id) {
			continue
		}
		getVolumeInformationMessage(writer, vi, verbosityLevel, dataNodeId)
	}
	return
}

func getVolumeInformationMessage(writer io.Writer, t *master_pb.VolumeInformationMessage, verbosityLevel int, dataNodeId string) {
	output(verbosityLevel >= 5, writer, "DataNodeId: %s; VolumeId: %d, volume Deletebytes: %d\n", dataNodeId, t.Id, t.DeletedByteCount)
	VolumeServerDeleteSizeGaugeShell.WithLabelValues(dataNodeId + fmt.Sprintf("-%d", t.Id)).Set(float64(t.DeletedByteCount))

	pusher := push.New("localhost:9091", "shell").Gatherer(GatherShell)
	err := pusher.Push()
	if err != nil {
		fmt.Println("Failed to push to gateway, " + err.Error())
	}

	return
}
