// Copyright 2018 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Scrape `ndbinfo.disk_write_speed*` tables.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

// TODO we may not need to have `thr_no` verbosity here. add less verbose metrics.

type ndbinfoDiskWriteSpeedLookup struct {
	nodeID string
	thrNo  string
}

type ndbinfoDiskWriteSpeedBytesWritten struct {
	backupLcp uint64
	redo      uint64
}

type ndbinfoDiskWriteSpeedCounterMap map[ndbinfoDiskWriteSpeedLookup]ndbinfoDiskWriteSpeedBytesWritten

func (self ndbinfoDiskWriteSpeedCounterMap) add(update ndbinfoDiskWriteSpeedCounterMap) {
	for key, value := range update {
		if entry, ok := self[key]; ok {
			entry.backupLcp += value.backupLcp
			entry.redo += value.redo
			self[key] = entry
		} else {
			self[key] = value
		}
	}
}

var ndbinfoDiskWriteSpeedCounter = make(ndbinfoDiskWriteSpeedCounterMap)

const ndbinfoDiskWriteSpeedQueryBase = `
	SELECT
		node_id,
		thr_no,
		backup_lcp_bytes_written,
		redo_bytes_written
	FROM ndbinfo.disk_write_speed_base
	ORDER BY node_id, thr_no, millis_ago;
`

const ndbinfoDiskWriteSpeedQueryAggregate = `
	SELECT
		node_id,
		thr_no,
		slowdowns_due_to_io_lag,
		slowdowns_due_to_high_cpu,
		disk_write_speed_set_to_min,
		current_target_disk_write_speed
	FROM ndbinfo.disk_write_speed_aggregate;
`

var (
	ndbinfoDiskWriteSpeedTaskRunning   bool
	ndbinfoDiskWriteSpeedTaskConn      *instance
	ndbinfoDiskWriteSpeedTaskLastError error
)

// Tunable flags.
var (
	ndbinfoDiskWriteSpeedVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoDiskWriteSpeed{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1")
)

// Metric descriptors.
var (
	ndbinfoDiskWriteSpeedDescBackupLCPSpeed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_backup_lcp_bytes"),
		"Number of bytes written to disk by backup and LCP processes.",
		[]string{"node_id", "thr_no"}, nil,
	)
	ndbinfoDiskWriteSpeedDescRedoSpeed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_redo_bytes"),
		"Number of bytes written to REDO log.",
		[]string{"node_id", "thr_no"}, nil,
	)
	ndbinfoDiskWriteSpeedDescSlowdownsDueToIoLag = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_slowdowns_io_lag"),
		"Number of seconds since last node start that disk writes were slowed due to REDO log I/O lag.",
		[]string{"node_id", "thr_no"}, nil,
	)
	ndbinfoDiskWriteSpeedDescSlowdownsDueToHighCPU = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_slowdowns_high_cpu"),
		"Number of seconds since last node start that disk writes were slowed due to high CPU usage.",
		[]string{"node_id", "thr_no"}, nil,
	)
	ndbinfoDiskWriteSpeedDescDiskWriteSpeedSetToMin = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_set_to_min"),
		"Number of seconds since last node start that disk write speed was set to minimum.",
		[]string{"node_id", "thr_no"}, nil,
	)
	ndbinfoDiskWriteSpeedDescCurrentTargetDistWriteSpeed = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "disk_write_speed_target_speed"),
		"Actual speed of disk writes per LDM thread (aggregated).",
		[]string{"node_id", "thr_no"}, nil,
	)
)

func ndbinfoDiskWriteSpeedTaskScrape(
	intervalSecs uint,
	logger *slog.Logger,
) (ndbinfoDiskWriteSpeedCounterMap, error) {
	interval := time.Duration(intervalSecs) * time.Second
	ctx, ctxCancel := context.WithTimeout(context.Background(), interval-time.Second)
	defer ctxCancel()

	db := ndbinfoDiskWriteSpeedTaskConn.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoDiskWriteSpeedQueryBase)
	if err != nil {
		return nil, fmt.Errorf("error while querying: %w", err)
	}
	defer rows.Close()

	sumLimit := make(map[ndbinfoDiskWriteSpeedLookup]uint)
	update := make(ndbinfoDiskWriteSpeedCounterMap)

	var (
		nodeID, thrNo                           string
		backupLcpBytesWritten, redoBytesWritten uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &thrNo,
			&backupLcpBytesWritten, &redoBytesWritten,
		); err != nil {
			return nil, fmt.Errorf("error while scanning: %w", err)
		}

		lookup := ndbinfoDiskWriteSpeedLookup{nodeID, thrNo}
		if sumLimit[lookup] >= intervalSecs {
			continue
		}
		if entry, ok := update[lookup]; ok {
			entry.backupLcp += backupLcpBytesWritten
			entry.redo += redoBytesWritten
			update[lookup] = entry
		} else {
			update[lookup] = ndbinfoDiskWriteSpeedBytesWritten{
				backupLcpBytesWritten,
				redoBytesWritten,
			}
		}
		sumLimit[lookup]++
	}

	return update, nil
}

func ndbinfoDiskWriteSpeedTask(intervalSecs uint, mysqlDSN string, logger *slog.Logger) {
	logger.Info("Background task started")
	ndbinfoDiskWriteSpeedTaskRunning = true
	defer func() {
		ndbinfoDiskWriteSpeedTaskRunning = false
		ndbinfoDiskWriteSpeedTaskConn = nil
		logger.Info("Background task stopped")
	}()

	instance, err := newInstance(mysqlDSN)
	if err != nil {
		logger.Error("Failed to create new DB connection instance", "err", err)
		return
	}
	ndbinfoDiskWriteSpeedTaskConn = instance

	interval := time.Duration(intervalSecs) * time.Second
	ticker := time.NewTicker(interval)

	for range ticker.C {
		logger.Debug("Scrape tick initiated")
		update, err := ndbinfoDiskWriteSpeedTaskScrape(intervalSecs, logger)
		if err != nil {
			logger.Error("Failed to scrape", "err", err)
			ndbinfoDiskWriteSpeedTaskLastError = fmt.Errorf("failed to scrape from background task: %w", err)
			return
		} else {
			ndbinfoDiskWriteSpeedTaskLastError = nil
		}
		ndbinfoDiskWriteSpeedCounter.add(update)
		logger.Debug("Scrape tick completed")
	}
}

// ScrapeNDBInfoDiskWriteSpeed collects from `ndbinfo.disk_write_speed*` tables.
type ScrapeNDBInfoDiskWriteSpeed struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoDiskWriteSpeed) Name() string {
	return ndbInfo + ".disk_write_speed"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoDiskWriteSpeed) Help() string {
	return "Collect metrics from ndbinfo.disk_write_speed* tables"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoDiskWriteSpeed) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoDiskWriteSpeed) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	if !ndbinfoDiskWriteSpeedTaskRunning {
		taskLogger := logger.With("context", "backgroundTask")
		// TODO make intervalSecs a configurable flag
		go ndbinfoDiskWriteSpeedTask(30, instance.getDSN(), taskLogger)
	}

	if ndbinfoDiskWriteSpeedTaskLastError != nil {
		err := ndbinfoDiskWriteSpeedTaskLastError
		logger.Error("Background task scrape failed", "err", err)
		ndbinfoDiskWriteSpeedTaskLastError = nil
		return err
	}

	for lookup, bytesWritten := range ndbinfoDiskWriteSpeedCounter {
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskWriteSpeedDescBackupLCPSpeed, prometheus.CounterValue, float64(bytesWritten.backupLcp),
			lookup.nodeID, lookup.thrNo,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoDiskWriteSpeedDescRedoSpeed, prometheus.CounterValue, float64(bytesWritten.redo),
			lookup.nodeID, lookup.thrNo,
		)
	}

	if *ndbinfoDiskWriteSpeedVerbosityFlag == "1" {
		db := instance.getDB()
		rows, err := db.QueryContext(ctx, ndbinfoDiskWriteSpeedQueryAggregate)
		if err != nil {
			return err
		}
		defer rows.Close()

		var (
			nodeID, thrNo                                       string
			slowdownsDueToIoLag, slowdownsDueToHighCPU          uint64
			diskWriteSpeedSetToMin, currentTargetDiskWriteSpeed uint64
		)
		for rows.Next() {
			if err := rows.Scan(
				&nodeID, &thrNo,
				&slowdownsDueToIoLag, &slowdownsDueToHighCPU,
				&diskWriteSpeedSetToMin, &currentTargetDiskWriteSpeed,
			); err != nil {
				return err
			}
			ch <- prometheus.MustNewConstMetric(
				ndbinfoDiskWriteSpeedDescSlowdownsDueToIoLag, prometheus.CounterValue, float64(slowdownsDueToIoLag),
				nodeID, thrNo,
			)
			ch <- prometheus.MustNewConstMetric(
				ndbinfoDiskWriteSpeedDescSlowdownsDueToHighCPU, prometheus.CounterValue, float64(slowdownsDueToHighCPU),
				nodeID, thrNo,
			)
			ch <- prometheus.MustNewConstMetric(
				ndbinfoDiskWriteSpeedDescDiskWriteSpeedSetToMin, prometheus.CounterValue, float64(diskWriteSpeedSetToMin),
				nodeID, thrNo,
			)
			ch <- prometheus.MustNewConstMetric(
				ndbinfoDiskWriteSpeedDescCurrentTargetDistWriteSpeed, prometheus.GaugeValue, float64(currentTargetDiskWriteSpeed),
				nodeID, thrNo,
			)
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoDiskWriteSpeed{}
