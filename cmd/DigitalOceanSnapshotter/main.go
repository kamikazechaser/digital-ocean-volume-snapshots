package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/digitalocean/godo"
	log "github.com/sirupsen/logrus"
)

const createdAtFormat = "2006-01-02T15:04:05Z"

type snapshotterContext struct {
	DoContext *DigitalOceanContext
}

func initLogging() {
	log.SetFormatter(&log.TextFormatter{
		DisableLevelTruncation: true,
	})

	log.SetOutput(os.Stdout)

	log.SetLevel(log.InfoLevel)
}

func main() {
	initLogging()

	DOToken, present := os.LookupEnv("DO_TOKEN")

	if !present {
		log.Fatal("Missing enviroment variable \"DO_TOKEN\"")
	}

	volumesEnv, present := os.LookupEnv("DO_VOLUMES")

	if !present {
		log.Fatal("Missing enviroment variable \"DO_VOLUMES\"")
	}

	snapshotCountEnv, present := os.LookupEnv("DO_SNAPSHOT_COUNT")

	if !present {
		log.Fatal("Missing enviroment variable \"DO_SNAPSHOT_COUNT\"")
	}

	snapshotCount, err := strconv.Atoi(snapshotCountEnv)

	if err != nil {
		log.Fatal("Enviroment variable \"DO_SNAPSHOT_COUNT\" is not an integer")
	}

	ctx := snapshotterContext{
		DoContext: &DigitalOceanContext{
			client: godo.NewFromToken(DOToken),
			ctx:    context.TODO(),
		},
	}

	volumeIDs := strings.Split(volumesEnv, ",")

	for _, volumeID := range volumeIDs {
		volume, _, err := ctx.DoContext.GetVolume(volumeID)
		if err != nil {
			handleError(ctx, err, true)
		}

		snapshot, _, err := ctx.DoContext.CreateSnapshot(&godo.SnapshotCreateRequest{
			VolumeID: volume.ID,
			Name:     time.Now().Format("2006-01-02T15:04:05"),
		})

		if err != nil {
			handleError(ctx, err, true)
		}

		log.Info(fmt.Sprintf("Created Snapshot with Id %s from volume %s", snapshot.ID, volume.Name))

		snapshots, _, err := ctx.DoContext.ListSnapshots(volume.ID, nil)

		if err != nil {
			handleError(ctx, err, true)
		}

		snapshotLength := len(snapshots)

		if snapshotLength > snapshotCount {
			sort.SliceStable(snapshots, func(firstIndex, secondIndex int) bool {
				firstTime, err := time.Parse(createdAtFormat, snapshots[firstIndex].Created)
				if err != nil {
					handleError(ctx, err, true)
				}

				secondTime, err := time.Parse(createdAtFormat, snapshots[secondIndex].Created)
				if err != nil {
					handleError(ctx, err, true)
				}

				return firstTime.Before(secondTime)
			})

			snapshotsToDelete := snapshotLength - snapshotCount

			for i := 0; i < snapshotsToDelete; i++ {
				snapshotToDeleteID := snapshots[i].ID
				_, err := ctx.DoContext.DeleteSnapshot(snapshotToDeleteID)
				if err != nil {
					handleError(ctx, err, false)
					return
				}

				log.Info(fmt.Sprintf("Deleted Snapshot with Id %s", snapshotToDeleteID))
			}
		}
	}

}

func handleError(ctx snapshotterContext, err error, fatal bool) {
	errString := err.Error()

	if fatal {
		log.Fatal(errString)
	}

	log.Error(errString)
}
