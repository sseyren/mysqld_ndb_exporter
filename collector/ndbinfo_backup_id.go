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

// Scrape `ndbinfo.backup_id`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

const ndbinfoBackupIDQuery = `
	SELECT
		id
	FROM ndbinfo.backup_id;
`

// Metric descriptors.
var (
	ndbinfoBackupIDDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "last_backup_id"),
		"The ID of the most recently initiated backup for this cluster.",
		nil, nil,
	)
)

// ScrapeNDBInfoBackupID collects from `ndbinfo.backup_id`.
type ScrapeNDBInfoBackupID struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoBackupID) Name() string {
	return ndbInfo + ".backup_id"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoBackupID) Help() string {
	return "Collect metrics from ndbinfo.backup_id"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoBackupID) Version() float64 {
	return 8.0
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoBackupID) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoBackupIDQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	var id uint64
	for rows.Next() {
		if err := rows.Scan(&id); err != nil {
			return err
		}
		ch <- prometheus.MustNewConstMetric(ndbinfoBackupIDDesc, prometheus.GaugeValue, float64(id))
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoBackupID{}
