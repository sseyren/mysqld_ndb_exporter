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

// Scrape `ndbinfo.tc_time_track_stats`.

package collector

import (
	"context"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

// TODO add more verbose metrics

// Added `WHERE block_number = 245` to make sure this is only DBTC as stated in docs.
const ndbinfoTCTimeTrackStatsQuery = `
	SELECT
		node_id,
		upper_bound,
		SUM(scans) AS scans,
		SUM(scan_errors) AS scan_errors,
		SUM(scan_fragments) AS scan_fragments,
		SUM(scan_fragment_errors) AS scan_fragment_errors,
		SUM(transactions) AS transactions,
		SUM(transaction_errors) AS transaction_errors,
		SUM(read_key_ops) AS read_key_ops,
		SUM(write_key_ops) AS write_key_ops,
		SUM(index_key_ops) AS index_key_ops,
		SUM(key_op_errors) AS key_op_errors
	FROM ndbinfo.tc_time_track_stats
	WHERE block_number = 245
	GROUP BY node_id, upper_bound;
`

// Metric descriptors.
var (
	ndbinfoTCTimeTrackStatsHistogramHelper ndbinfoHistogramHelper = []float64{50, 75, 112, 168, 252, 378, 567, 850, 1275, 1912, 2868, 4302, 6453, 9679, 14518, 21777, 32665, 48997, 73495, 110242, 165363, 248044, 372066, 558099, 837148, 1255722, 1883583, 2825374, 4238061, 6357091, 9535636, 14303454}

	ndbinfoTCTimeTrackStatsDescScans = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_scans"),
		"Total number of successful scans on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescScanErrors = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_scan_errors"),
		"Total number of failed scans on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescScanFragments = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_scan_fragments"),
		"Total number of successful fragment scans on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescScanFragmentErrors = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_scan_fragment_errors"),
		"Total number of failed fragment scans on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescTransactions = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_transactions"),
		"Total number of successful transactions on all DBTC block instances by latency as μs. Stateless transactions are not included.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescTransactionErrors = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_transaction_errors"),
		"Total number of failed transactions on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescReadKeyOps = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_read_key_ops"),
		"Total number of successful primary key reads on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescWriteKeyOps = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_write_key_ops"),
		"Total number of successful primary key writes on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescIndexKeyOps = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_index_key_ops"),
		"Total number of successful unique index key operations on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
	ndbinfoTCTimeTrackStatsDescKeyOpErrors = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, ndbInfo, "tc_time_track_stats_key_op_errors"),
		"Total number of unsuccessful key read or write operations on all DBTC block instances by latency as μs.",
		[]string{"node_id"}, nil,
	)
)

// ScrapeNDBInfoTCTimeTrackStats collects from `ndbinfo.tc_time_track_stats`.
type ScrapeNDBInfoTCTimeTrackStats struct{}

// Name of the Scraper. Should be unique.
func (ScrapeNDBInfoTCTimeTrackStats) Name() string {
	return ndbInfo + ".tc_time_track_stats"
}

// Help describes the role of the Scraper.
func (ScrapeNDBInfoTCTimeTrackStats) Help() string {
	return "Collect metrics from ndbinfo.tc_time_track_stats"
}

// Version of MySQL from which scraper is available.
func (ScrapeNDBInfoTCTimeTrackStats) Version() float64 {
	return 5.6
}

// Scrape collects data from database connection and sends it over channel as prometheus metric.
func (ScrapeNDBInfoTCTimeTrackStats) Scrape(ctx context.Context, instance *instance, ch chan<- prometheus.Metric, logger *slog.Logger) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, ndbinfoTCTimeTrackStatsQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	lastUpperBound := ndbinfoTCTimeTrackStatsHistogramHelper.GetLast()
	var (
		nodeID                                                   string
		upperBound                                               float64
		scans, scanErrors, scanFragments, scanFragmentErrors     uint64
		transactions, transactionErrors, readKeyOps, writeKeyOps uint64
		indexKeyOps, keyOpErrors                                 uint64
	)
	// Histogram buckets by nodeID
	scansBuckets := map[string]map[float64]uint64{}
	scanErrorsBuckets := map[string]map[float64]uint64{}
	scanFragmentsBuckets := map[string]map[float64]uint64{}
	scanFragmentErrorsBuckets := map[string]map[float64]uint64{}
	transactionsBuckets := map[string]map[float64]uint64{}
	transactionErrorsBuckets := map[string]map[float64]uint64{}
	readKeyOpsBuckets := map[string]map[float64]uint64{}
	writeKeyOpsBuckets := map[string]map[float64]uint64{}
	indexKeyOpsBuckets := map[string]map[float64]uint64{}
	keyOpErrorsBuckets := map[string]map[float64]uint64{}

	for rows.Next() {
		if err := rows.Scan(
			&nodeID, &upperBound,
			&scans, &scanErrors, &scanFragments, &scanFragmentErrors,
			&transactions, &transactionErrors, &readKeyOps, &writeKeyOps,
			&indexKeyOps, &keyOpErrors,
		); err != nil {
			return err
		}
		if scansBuckets[nodeID] == nil {
			scansBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		scansBuckets[nodeID][upperBound] = scans

		if scanErrorsBuckets[nodeID] == nil {
			scanErrorsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		scanErrorsBuckets[nodeID][upperBound] = scanErrors

		if scanFragmentsBuckets[nodeID] == nil {
			scanFragmentsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		scanFragmentsBuckets[nodeID][upperBound] = scanFragments

		if scanFragmentErrorsBuckets[nodeID] == nil {
			scanFragmentErrorsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		scanFragmentErrorsBuckets[nodeID][upperBound] = scanFragmentErrors

		if transactionsBuckets[nodeID] == nil {
			transactionsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		transactionsBuckets[nodeID][upperBound] = transactions

		if transactionErrorsBuckets[nodeID] == nil {
			transactionErrorsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		transactionErrorsBuckets[nodeID][upperBound] = transactionErrors

		if readKeyOpsBuckets[nodeID] == nil {
			readKeyOpsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		readKeyOpsBuckets[nodeID][upperBound] = readKeyOps

		if writeKeyOpsBuckets[nodeID] == nil {
			writeKeyOpsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		writeKeyOpsBuckets[nodeID][upperBound] = writeKeyOps

		if indexKeyOpsBuckets[nodeID] == nil {
			indexKeyOpsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		indexKeyOpsBuckets[nodeID][upperBound] = indexKeyOps

		if keyOpErrorsBuckets[nodeID] == nil {
			keyOpErrorsBuckets[nodeID] = make(map[float64]uint64, 32)
		}
		keyOpErrorsBuckets[nodeID][upperBound] = keyOpErrors
	}

	for nodeID, bucket := range scansBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescScans, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range scanErrorsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescScanErrors, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range scanFragmentsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescScanFragments, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range scanFragmentErrorsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescScanFragmentErrors, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range transactionsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescTransactions, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range transactionErrorsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescTransactionErrors, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range readKeyOpsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescReadKeyOps, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range writeKeyOpsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescWriteKeyOps, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range indexKeyOpsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescIndexKeyOps, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}
	for nodeID, bucket := range keyOpErrorsBuckets {
		sum := ndbinfoTCTimeTrackStatsHistogramHelper.ArtificialSum(bucket)
		ndbinfoTCTimeTrackStatsHistogramHelper.MakeCumulative(bucket)
		ch <- prometheus.MustNewConstHistogram(ndbinfoTCTimeTrackStatsDescKeyOpErrors, bucket[lastUpperBound], sum, bucket,
			nodeID,
		)
	}

	return nil
}

// check interface
var _ Scraper = ScrapeNDBInfoTCTimeTrackStats{}
