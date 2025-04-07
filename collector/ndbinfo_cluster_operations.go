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

// Scrape `ndbinfo.cluster_operations`.

package collector

import (
	"context"
	"log/slog"

	"github.com/alecthomas/kingpin/v2"
	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoClusterOperationsQuery = `
	SELECT
		node_id,
		operation_type,
		IFNULL(state, '') AS state,
		COUNT(*) AS count
	FROM ndbinfo.cluster_operations
	GROUP BY node_id, operation_type, state;
`

const ndbinfoClusterOperationsQueryFull = `
	SELECT
		node_id,
		operation_type,
		IFNULL(state, '') AS state,
		tableid,
		COUNT(*) AS count
	FROM ndbinfo.cluster_operations
	GROUP BY node_id, operation_type, state, tableid;
`

// Tunable flags.
var (
	ndbinfoClusterOperationsVerbosityFlag = kingpin.Flag(
		"collect."+ScrapeNDBInfoClusterOperations{}.Name()+".verbose",
		"Metrics verbosity",
	).Default("0").Enum("0", "1")
)

// Metric descriptors.
var (
	ndbinfoClusterOperationsDescCount = NewVariablePromDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "cluster_operations_count"),
		"Total count of current cluster operations.",
		prometheus.GaugeValue,
		[]string{"node_id", "operation_type", "state"},
		[]string{"node_id", "operation_type", "state", "tableid"},
	)
)

// ScrapeNDBInfoClusterOperations collects from `ndbinfo.cluster_operations`.
type ScrapeNDBInfoClusterOperations struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoClusterOperations) Name() string {
	return ndbInfo + ".cluster_operations"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoClusterOperations) Help() string {
	return "Collect metrics from ndbinfo.cluster_operations"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoClusterOperations) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (self ScrapeNDBInfoClusterOperations) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	switch *ndbinfoClusterOperationsVerbosityFlag {
	case "1":
		return self.ScrapeVerbose1(ctx, instance, ch, logger)
	default:
		return self.ScrapeVerbose0(ctx, instance, ch, logger)
	}
}

func (ScrapeNDBInfoClusterOperations) ScrapeVerbose0(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoClusterOperationsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, operationType, state string
		count                        uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &operationType, &state,
			&count,
		); err != nil {
			return err
		}
		ch <- ndbinfoClusterOperationsDescCount.Metric(float64(count), nodeID, operationType, state)
	}

	return nil
}

func (ScrapeNDBInfoClusterOperations) ScrapeVerbose1(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoClusterOperationsQueryFull)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, operationType, state, tableID string
		count                                 uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &operationType, &state, &tableID,
			&count,
		); err != nil {
			return err
		}
		ch <- ndbinfoClusterOperationsDescCount.Metric(float64(count), nodeID, operationType, state, tableID)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoClusterOperations{}
