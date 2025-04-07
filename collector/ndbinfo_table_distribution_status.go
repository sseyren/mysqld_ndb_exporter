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

// Scrape `ndbinfo.table_distribution_status`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoTableDistributionQuery = `
	SELECT
		node_id,
		table_id,
		tab_copy_status,
		tab_update_status,
		tab_lcp_status,
		tab_status,
		tab_partitions,
		tab_fragments,
		current_scan_count,
		scan_count_wait,
		is_reorg_ongoing
	FROM ndbinfo.table_distribution_status;
`

// Tunable flags.
var (
	ndbinfoTableDistributionSkipIdleFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoTableDistribution{}.Name()+".skip_idle",
		"Skip printing metrics if value of metric is `IDLE`",
	).Default("true").Bool()
)

// Metric descriptors.
var (
	ndbinfoTableDistributionDescCopyStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_copy_status"),
		"Status of copying of table distribution data to disk. Values are; 0=IDLE 1=SR_PHASE1_READ_PAGES 2=SR_PHASE2_READ_TABLE 3=SR_PHASE3_COPY_TABLE 4=REMOVE_NODE 5=LCP_READ_TABLE 6=COPY_TAB_REQ 7=COPY_NODE_STATE 8=ADD_TABLE_COORDINATOR 9=ADD_TABLE_PARTICIPANT 10=INVALIDATE_NODE_LCP 11=ALTER_TABLE 12=COPY_TO_SAVE 13=GET_TABINFO",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescUpdateStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_update_status"),
		"Status of updating of table distribution data. Values are; 0=IDLE 1=LOCAL_CHECKPOINT 2=LOCAL_CHECKPOINT_QUEUED 3=REMOVE_NODE 4=COPY_TAB_REQ 5=ADD_TABLE_COORDINATOR 6=ADD_TABLE_PARTICIPANT 7=INVALIDATE_NODE_LCP 8=CALLBACK",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescLCPStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_lcp_status"),
		"Status of table LCP. Values are; 1=ACTIVE 2=wRITING_TO_FILE 3=COMPLETED",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_status"),
		"Table internal status. Values are; 0=IDLE 1=ACTIVE 2=CREATING 3=DROPPING",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescPartitions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_partitions"),
		"Number of partitions in table.",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescFragments = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_fragments"),
		"Number of fragments in table.",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescScansCurrent = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_scans_current"),
		"Current number of active scans.",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescScansWaiting = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_scans_waiting"),
		"Current number of scans waiting to be performed before ALTER TABLE can complete.",
		[]string{"node_id", "table_id"}, nil,
	)
	ndbinfoTableDistributionDescReorgOngoing = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "table_distribution_reorg_ongoing"),
		"Whether the table is currently being reorganized; 1=yes 0=no",
		[]string{"node_id", "table_id"}, nil,
	)
)

// ScrapeNDBInfoTableDistribution collects from `ndbinfo.table_distribution_status`.
type ScrapeNDBInfoTableDistribution struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoTableDistribution) Name() string {
	return ndbInfo + ".table_distribution_status"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoTableDistribution) Help() string {
	return "Collect metrics from ndbinfo.table_distribution_status"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoTableDistribution) Version() float64 {
	return 5.7
}

func ndbinfoTableDistributionCopyStatusToNumber(status string) float64 {
	switch status {
	case "IDLE":
		return 0
	case "SR_PHASE1_READ_PAGES":
		return 1
	case "SR_PHASE2_READ_TABLE":
		return 2
	case "SR_PHASE3_COPY_TABLE":
		return 3
	case "REMOVE_NODE":
		return 4
	case "LCP_READ_TABLE":
		return 5
	case "COPY_TAB_REQ":
		return 6
	case "COPY_NODE_STATE":
		return 7
	case "ADD_TABLE_COORDINATOR":
		return 8
	case "ADD_TABLE_PARTICIPANT":
		return 9
	case "INVALIDATE_NODE_LCP":
		return 10
	case "ALTER_TABLE":
		return 11
	case "COPY_TO_SAVE":
		return 12
	case "GET_TABINFO":
		return 13
	default:
		return -1
	}
}

func ndbinfoTableDistributionUpdateStatusToNumber(status string) float64 {
	switch status {
	case "IDLE":
		return 0
	case "LOCAL_CHECKPOINT":
		return 1
	case "LOCAL_CHECKPOINT_QUEUED":
		return 2
	case "REMOVE_NODE":
		return 3
	case "COPY_TAB_REQ":
		return 4
	case "ADD_TABLE_COORDINATOR":
		return 5
	case "ADD_TABLE_PARTICIPANT":
		return 6
	case "INVALIDATE_NODE_LCP":
		return 7
	case "CALLBACK":
		return 8
	default:
		return -1
	}
}

func ndbinfoTableDistributionLCPStatusToNumber(status string) float64 {
	switch status {
	case "ACTIVE":
		return 1
	// This is not a typo.
	case "wRITING_TO_FILE":
		return 2
	case "COMPLETED":
		return 3
	default:
		return -1
	}
}

func ndbinfoTableDistributionStatusToNumber(status string) float64 {
	switch status {
	case "IDLE":
		return 0
	case "ACTIVE":
		return 1
	case "CREATING":
		return 2
	case "DROPPING":
		return 3
	default:
		return -1
	}
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoTableDistribution) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoTableDistributionQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, tableID, tabCopyStatus, tabUpdateStatus, tabLcpStatus, tabStatus string
		tabPartitions, tabFragments, currentScanCount, scanCountWait             uint64
		isReorgOngoing                                                           uint8
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &tableID, &tabCopyStatus, &tabUpdateStatus, &tabLcpStatus, &tabStatus,
			&tabPartitions, &tabFragments, &currentScanCount, &scanCountWait,
			&isReorgOngoing,
		); err != nil {
			return err
		}

		copyStatus := ndbinfoTableDistributionCopyStatusToNumber(tabCopyStatus)
		if *ndbinfoTableDistributionSkipIdleFlag == false || copyStatus != 0 { // IDLE
			ch <- prometheus.MustNewConstMetric(
				ndbinfoTableDistributionDescCopyStatus, prometheus.GaugeValue, copyStatus,
				nodeID, tableID,
			)
		}

		updateStatus := ndbinfoTableDistributionUpdateStatusToNumber(tabUpdateStatus)
		if *ndbinfoTableDistributionSkipIdleFlag == false || updateStatus != 0 { // IDLE
			ch <- prometheus.MustNewConstMetric(
				ndbinfoTableDistributionDescUpdateStatus, prometheus.GaugeValue, updateStatus,
				nodeID, tableID,
			)
		}

		lcpStatus := ndbinfoTableDistributionLCPStatusToNumber(tabLcpStatus)
		if *ndbinfoTableDistributionSkipIdleFlag == false || lcpStatus != 3 { // COMPLETED
			ch <- prometheus.MustNewConstMetric(
				ndbinfoTableDistributionDescLCPStatus, prometheus.GaugeValue, lcpStatus,
				nodeID, tableID,
			)
		}

		status := ndbinfoTableDistributionStatusToNumber(tabStatus)
		if *ndbinfoTableDistributionSkipIdleFlag == false || status != 0 { // IDLE
			ch <- prometheus.MustNewConstMetric(
				ndbinfoTableDistributionDescStatus, prometheus.GaugeValue, status,
				nodeID, tableID,
			)
		}

		ch <- prometheus.MustNewConstMetric(
			ndbinfoTableDistributionDescPartitions, prometheus.GaugeValue, float64(tabPartitions),
			nodeID, tableID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTableDistributionDescFragments, prometheus.GaugeValue, float64(tabFragments),
			nodeID, tableID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTableDistributionDescScansCurrent, prometheus.GaugeValue, float64(currentScanCount),
			nodeID, tableID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTableDistributionDescScansWaiting, prometheus.GaugeValue, float64(scanCountWait),
			nodeID, tableID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTableDistributionDescReorgOngoing, prometheus.GaugeValue, float64(isReorgOngoing),
			nodeID, tableID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoTableDistribution{}
