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

package collector

import (
	"database/sql"
	"math"

	"github.com/prometheus/client_golang/prometheus"
)

// ndbInfo subsystem.
const ndbInfo = "ndbinfo"

type number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

// Returns sum of slice values.
func sumSlice[T number](slice []T) T {
	var total T
	for _, value := range slice {
		total += value
	}
	return total
}

// Alias type for upper bound values.
type ndbinfoHistogramHelper []float64

// Returns last upper bound value from array.
func (upperBounds ndbinfoHistogramHelper) GetLast() float64 {
	return upperBounds[len(upperBounds)-1]
}

// Artificially generates histogram summary by assuming every counter is average.
// This func assumes histogram bucket is not cumulative, so this needs to be called before `MakeCumulative`.
func (upperBounds ndbinfoHistogramHelper) ArtificialSum(bucket map[float64]uint64) float64 {
	var prev, sum float64 = 0, 0
	for _, bound := range upperBounds {
		sum += ((bound + prev) / 2.0) * float64(bucket[bound])
		prev = bound
	}
	return sum
}

// Makes this histogram bucket cumulative. Modifies bucket itself.
func (upperBounds ndbinfoHistogramHelper) MakeCumulative(bucket map[float64]uint64) {
	var sum uint64 = 0
	for _, bound := range upperBounds {
		bucket[bound] += sum
		sum = bucket[bound]
	}
}

// Wrapper for `prometheus.Desc` but allows multiple label variables instead of fixed number of labels.
type VariablePromDesc struct {
	FQName         string
	Help           string
	ValueType      prometheus.ValueType
	PossibleLabels [][]string
}

func NewVariablePromDesc(fqName string, help string, valueType prometheus.ValueType, possibleLabels ...[]string) VariablePromDesc {
	return VariablePromDesc{FQName: fqName, Help: help, ValueType: valueType, PossibleLabels: possibleLabels}
}

func (vDesc *VariablePromDesc) Metric(value float64, labelValues ...string) prometheus.Metric {
	var labels []string
	for _, l := range vDesc.PossibleLabels {
		if len(l) == len(labelValues) {
			labels = l
			break
		}
	}
	desc := prometheus.NewDesc(vDesc.FQName, vDesc.Help, labels, nil)
	return prometheus.MustNewConstMetric(desc, vDesc.ValueType, value, labelValues...)
}

// Returns actual value as float64 if value is valid, returns NaN if it's not.
func unpackNullAsNaN[T number](value sql.Null[T]) float64 {
	if value.Valid {
		return float64(value.V)
	} else {
		return math.NaN()
	}
}
