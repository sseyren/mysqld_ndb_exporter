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

// Scrape `ndbinfo.nodes`.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoNodesQuery = `
	SELECT
		node_id,
		uptime,
		status,
		start_phase,
		config_generation
	FROM ndbinfo.nodes;
`

// Metric descriptors.
var (
	ndbinfoNodesDescUptime = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "nodes_uptime"),
		"Time since the node was last started, in seconds.",
		[]string{"node_id"}, nil,
	)
	ndbinfoNodesDescStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "nodes_status"),
		"Current status of the data node. Values are; 0=NOTHING 1=CMVMI 2=STARTING 3=STARTED 4=SINGLEUSER 5=STOPPING_1 6=STOPPING_2 7=STOPPING_3 8=STOPPING_4",
		[]string{"node_id"}, nil,
	)
	ndbinfoNodesDescConfigGeneration = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "nodes_config_generation"),
		"The version of the cluster configuration file in use on this data node.",
		[]string{"node_id"}, nil,
	)
)

// ScrapeNDBInfoNodes collects from `ndbinfo.nodes`.
type ScrapeNDBInfoNodes struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoNodes) Name() string {
	return ndbInfo + ".nodes"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoNodes) Help() string {
	return "Collect metrics from ndbinfo.nodes"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoNodes) Version() float64 {
	return 5.6
}

func ndbinfoNodesStatusToNumber(status string, startPhase int64) float64 {
	switch status {
	case "NOTHING":
		return 0
	case "CMVMI":
		return 1
	case "STARTING":
		if startPhase <= 0 {
			return 2
		}
		status, _ := strconv.ParseFloat(
			fmt.Sprintf("2.%d", startPhase),
			64,
		)
		return status
	case "STARTED":
		return 3
	case "SINGLEUSER":
		return 4
	case "STOPPING_1":
		return 5
	case "STOPPING_2":
		return 6
	case "STOPPING_3":
		return 7
	case "STOPPING_4":
		return 8
	default:
		return -1
	}
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoNodes) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoNodesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID           string
		uptime           uint64
		status           string
		startPhase       int64
		configGeneration uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&nodeID,
			&uptime,
			&status, &startPhase,
			&configGeneration,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoNodesDescUptime, prometheus.CounterValue, float64(uptime),
			nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoNodesDescStatus, prometheus.GaugeValue, ndbinfoNodesStatusToNumber(status, startPhase),
			nodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoNodesDescConfigGeneration, prometheus.GaugeValue, float64(configGeneration),
			nodeID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoNodes{}
