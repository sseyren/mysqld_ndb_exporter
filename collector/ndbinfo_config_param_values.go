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

// Scrape `ndbinfo.config_params` and `ndbinfo.config_values`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoConfigValuesQuery = `
	SELECT
		p.param_name AS name,
		v.node_id AS node_id,
		v.config_value AS value
	FROM ndbinfo.config_params AS p
	JOIN ndbinfo.config_values AS v
		ON p.param_number = v.config_param
	WHERE p.param_type in ('unsigned', 'bool', 'enum');
`

// Metric descriptors.
var (
	ndbinfoConfigValuesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "config_value"),
		"Current state of node configuration parameter values.",
		[]string{"name", "node_id"}, nil,
	)
)

// ScrapeNDBInfoConfigValues collects from `ndbinfo.config_params` and `config_values`.
type ScrapeNDBInfoConfigValues struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoConfigValues) Name() string {
	return ndbInfo + ".config_values"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoConfigValues) Help() string {
	return "Collect metrics from ndbinfo.config_params and ndbinfo.config_values"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoConfigValues) Version() float64 {
	return 5.7
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoConfigValues) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoConfigValuesQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var (
		name, nodeID string
		value        uint64
	)
	for rows.Next() {
		if err := rows.Scan(
			&name,
			&nodeID, &value,
		); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(
			ndbinfoConfigValuesDesc, prometheus.GaugeValue, float64(value),
			name, nodeID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoConfigValues{}
