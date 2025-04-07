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

// Scrape `ndbinfo.cluster_transactions`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoClusterTransactionsQuery = `
	SELECT
		node_id,
		IFNULL(state, '') AS state,
		MAX(inactive_seconds) AS max_inactive_seconds,
		COUNT(DISTINCT transid) AS count
	FROM ndbinfo.cluster_transactions
	GROUP BY node_id, state;
`

// Metric descriptors.
var (
	ndbinfoClusterTransactionsDescCount = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "cluster_transactions_count"),
		"Total count of transactions.",
		[]string{"node_id", "state"}, nil,
	)
	ndbinfoClusterTransactionsDescMaxInactivity = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "cluster_transactions_max_inactive_seconds"),
		"Longest inactive transaction time in the entire cluster, in seconds.",
		nil, nil,
	)
)

// ScrapeNDBInfoClusterTransactions collects from `ndbinfo.cluster_transactions`.
type ScrapeNDBInfoClusterTransactions struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoClusterTransactions) Name() string {
	return ndbInfo + ".cluster_transactions"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoClusterTransactions) Help() string {
	return "Collect metrics from ndbinfo.cluster_transactions"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoClusterTransactions) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoClusterTransactions) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoClusterTransactionsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, state             string
		maxInactiveSeconds, count uint64
	)
	var totalMaxInactiveSeconds *uint64 = nil
	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &state,
			&maxInactiveSeconds, &count,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoClusterTransactionsDescCount, prometheus.GaugeValue, float64(count),
			nodeID, state,
		)
		if totalMaxInactiveSeconds == nil || maxInactiveSeconds > *totalMaxInactiveSeconds {
			new := maxInactiveSeconds
			totalMaxInactiveSeconds = &new
		}
	}
	if totalMaxInactiveSeconds != nil {
		ch <- prometheus.MustNewConstMetric(ndbinfoClusterTransactionsDescMaxInactivity, prometheus.GaugeValue, float64(*totalMaxInactiveSeconds))
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoClusterTransactions{}
