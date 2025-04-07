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

// Scrape `ndbinfo.files` (aka. `information_schema.FILES`).

package collector

import (
	"context"
	"database/sql"
	"log/slog"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoFilesQuery = `
	SELECT
		FILE_ID,
		FILE_NAME,
		FILE_TYPE,
		IFNULL(TABLESPACE_NAME, '') AS TABLESPACE_NAME,
		IFNULL(LOGFILE_GROUP_NAME, '') AS LOGFILE_GROUP_NAME,
		FREE_EXTENTS,
		TOTAL_EXTENTS,
		EXTENT_SIZE,
		MAXIMUM_SIZE,
		VERSION,
		IFNULL(EXTRA, '') AS EXTRA
	FROM information_schema.FILES
	WHERE
		ENGINE = 'ndbcluster'
		AND FILE_ID IS NOT NULL
		AND FILE_NAME IS NOT NULL
		AND FILE_TYPE IS NOT NULL;
`

// Metric descriptors.
var (
	ndbinfoFilesLabels     = []string{"file_id", "file_name", "file_type", "tablespace", "logfile_group"}
	ndbinfoFilesLabelsFull = append(ndbinfoFilesLabels, "node_id")

	ndbinfoFilesDescFreeExtents = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_free_extents"),
		"The number of extents which have not yet been used by the file.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
	ndbinfoFilesDescTotalExtents = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_total_extents"),
		"The total number of extents allocated to the file.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
	ndbinfoFilesDescExtentSize = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_extent_size"),
		"The size of an extent for the file in bytes.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
	ndbinfoFilesDescSize = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_size"),
		"The size of the file in bytes.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
	ndbinfoFilesDescVersion = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_version"),
		"The version number of the file.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
	ndbinfoFilesDescUndoLogBufferSize = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "files_undo_log_buffer_size"),
		"The undo log buffer size.",
		prometheus.GaugeValue,
		ndbinfoFilesLabels, ndbinfoFilesLabelsFull,
	)
)

// ScrapeNDBInfoFiles collects from `ndbinfo.files` (aka. `information_schema.FILES`).
type ScrapeNDBInfoFiles struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoFiles) Name() string {
	return ndbInfo + ".files"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoFiles) Help() string {
	return "Collect metrics from ndbinfo.files (aka. information_schema.FILES)"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoFiles) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoFiles) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoFilesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		fileID, fileName, fileType, tablespaceName, logfileGroupName string
		freeExtents, totalExtents, extentSize, maximumSize, version  sql.Null[uint64]
		extra                                                        string
	)
	var (
		nodeID         string
		undoBufferSize int64 = -1
	)
	for rows.Next() {
		if err := rows.Scan(
			&fileID, &fileName, &fileType, &tablespaceName, &logfileGroupName,
			&freeExtents, &totalExtents, &extentSize, &maximumSize, &version,
			&extra,
		); err != nil {
			return err
		}

		// parsing `EXTRA` column
		extra = strings.TrimSpace(extra)
		if extra != "" {
			for _, entry := range strings.Split(extra, ";") {
				kv := strings.SplitN(entry, "=", 2)
				if len(kv) != 2 {
					// TODO log as warning
					continue
				}
				key, valueStr := kv[0], kv[1]
				switch key {
				case "CLUSTER_NODE":
					nodeID = valueStr
				case "UNDO_BUFFER_SIZE":
					value, err := strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						// TODO log as warning
					} else {
						undoBufferSize = value
					}
				default:
					// TODO log as warning
				}
			}
		}

		if nodeID == "" {
			ch <- ndbinfoFilesDescFreeExtents.Metric(unpackNullAsNaN(freeExtents), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			ch <- ndbinfoFilesDescTotalExtents.Metric(unpackNullAsNaN(totalExtents), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			ch <- ndbinfoFilesDescExtentSize.Metric(unpackNullAsNaN(extentSize), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			ch <- ndbinfoFilesDescSize.Metric(unpackNullAsNaN(maximumSize), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			ch <- ndbinfoFilesDescVersion.Metric(unpackNullAsNaN(version), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			if fileType == "UNDO LOG" && undoBufferSize != -1 {
				ch <- ndbinfoFilesDescUndoLogBufferSize.Metric(float64(undoBufferSize), fileID, fileName, fileType, tablespaceName, logfileGroupName)
			}
		} else {
			ch <- ndbinfoFilesDescFreeExtents.Metric(unpackNullAsNaN(freeExtents), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			ch <- ndbinfoFilesDescTotalExtents.Metric(unpackNullAsNaN(totalExtents), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			ch <- ndbinfoFilesDescExtentSize.Metric(unpackNullAsNaN(extentSize), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			ch <- ndbinfoFilesDescSize.Metric(unpackNullAsNaN(maximumSize), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			ch <- ndbinfoFilesDescVersion.Metric(unpackNullAsNaN(version), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			if fileType == "UNDO LOG" && undoBufferSize != -1 {
				ch <- ndbinfoFilesDescUndoLogBufferSize.Metric(float64(undoBufferSize), fileID, fileName, fileType, tablespaceName, logfileGroupName, nodeID)
			}
		}

	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoFiles{}
