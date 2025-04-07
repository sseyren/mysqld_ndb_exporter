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

// Scrape `ndbinfo.transporters`.

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO add `ndbinfo.transporter_details` for more verbose metrics?

const ndbinfoTransportersQuerySelectFields = `
	node_id,
	remote_node_id,
	status,
	remote_address,
	bytes_sent,
	bytes_received,
	overloaded,
	slowdown
`

// TODO add `WHERE remote_address != '-' or status != 'CONNECTING'`
const ndbinfoTransportersQueryTemplate = `
	SELECT %s
	FROM ndbinfo.transporters;
`

// Metric descriptors.
var (
	ndbinfoTransportersScrapeEncrypted = true

	ndbinfoTransportersDescStatus = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_status"),
		"Current status of connection. Values are; 0=CONNECTED 1=CONNECTING 2=DISCONNECTED 3=DISCONNECTING",
		[]string{"node_id", "remote_node_id", "remote_address"}, nil,
	)
	ndbinfoTransportersDescBytesSent = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_bytes_sent"),
		"Number of bytes sent using this connection.",
		[]string{"node_id", "remote_node_id"}, nil,
	)
	ndbinfoTransportersDescBytesReceived = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_bytes_received"),
		"Number of bytes received using this connection.",
		[]string{"node_id", "remote_node_id"}, nil,
	)
	ndbinfoTransportersDescOverloaded = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_overloaded"),
		"1 if this transporter is currently overloaded, otherwise 0.",
		[]string{"node_id", "remote_node_id"}, nil,
	)
	ndbinfoTransportersDescSlowdown = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_slowdown"),
		"1 if this transporter is in slowdown state, otherwise 0.",
		[]string{"node_id", "remote_node_id"}, nil,
	)
	ndbinfoTransportersDescEncrypted = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "transporters_encrypted"),
		"If this transporter is connected using TLS, this column is 1, otherwise it is 0.",
		[]string{"node_id", "remote_node_id"}, nil,
	)
)

// ScrapeNDBInfoTransporters collects from `ndbinfo.transporters`.
type ScrapeNDBInfoTransporters struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoTransporters) Name() string {
	return ndbInfo + ".transporters"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoTransporters) Help() string {
	return "Collect metrics from ndbinfo.transporters"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoTransporters) Version() float64 {
	return 5.6
}

func ndbinfoTransportersStatusToNumber(status string) float64 {
	switch status {
	case "CONNECTED":
		return 0
	case "CONNECTING":
		return 1
	case "DISCONNECTED":
		return 2
	case "DISCONNECTING":
		return 3
	default:
		return -1
	}
}

func ndbinfoTransportersQueryDB(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
	if ndbinfoTransportersScrapeEncrypted {
		query := fmt.Sprintf(ndbinfoTransportersQueryTemplate, ndbinfoTransportersQuerySelectFields+", encrypted")
		rows, err := db.QueryContext(ctx, query)
		if err == nil {
			return rows, nil
		} else {
			ndbinfoTransportersScrapeEncrypted = false
			return ndbinfoTransportersQueryDB(ctx, db)
		}
	} else {
		query := fmt.Sprintf(ndbinfoTransportersQueryTemplate, ndbinfoTransportersQuerySelectFields)
		return db.QueryContext(ctx, query)
	}
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoTransporters) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := ndbinfoTransportersQueryDB(ctx, db)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		nodeID, remoteNodeID, status, remoteAddress string
		bytesSent, bytesReceived                    uint64
		overloaded, slowdown, encrypted             uint8
	)
	vars := []any{
		&nodeID, &remoteNodeID, &status, &remoteAddress,
		&bytesSent, &bytesReceived,
		&overloaded, &slowdown,
	}
	if ndbinfoTransportersScrapeEncrypted {
		vars = append(vars, &encrypted)
	}

	for rows.Next() {
		if err := rows.Scan(vars...); err != nil {
			return err
		}
		// TODO remove this if condition
		// Skip configured but not connected node IDs
		if remoteAddress == "-" && status == "CONNECTING" {
			continue
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersDescStatus, prometheus.GaugeValue, ndbinfoTransportersStatusToNumber(status),
			nodeID, remoteNodeID, remoteAddress,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersDescBytesSent, prometheus.CounterValue, float64(bytesSent),
			nodeID, remoteNodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersDescBytesReceived, prometheus.CounterValue, float64(bytesReceived),
			nodeID, remoteNodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersDescOverloaded, prometheus.GaugeValue, float64(overloaded),
			nodeID, remoteNodeID,
		)
		ch <- prometheus.MustNewConstMetric(
			ndbinfoTransportersDescSlowdown, prometheus.GaugeValue, float64(slowdown),
			nodeID, remoteNodeID,
		)
		if ndbinfoTransportersScrapeEncrypted {
			ch <- prometheus.MustNewConstMetric(
				ndbinfoTransportersDescEncrypted, prometheus.GaugeValue, float64(encrypted),
				nodeID, remoteNodeID,
			)
		}
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoTransporters{}
