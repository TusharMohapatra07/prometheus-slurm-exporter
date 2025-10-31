package exporter

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

var MockGpuSinfoScraper = &MockScraper{fixture: "fixtures/sinfo_gpu_out.json"}
var MockGpuSacctScraper = &MockScraper{fixture: "fixtures/sacct_gpu_out.json"}
var MockGpuSinfoFallbackScraper = &MockScraper{fixture: "fixtures/sinfo_gpu_fallback.txt"}
var MockGpuSacctFallbackScraper = &MockScraper{fixture: "fixtures/sacct_gpu_fallback.txt"}

func TestParseGresGpuCount(t *testing.T) {
	tests := []struct {
		name     string
		gres     string
		expected float64
	}{
		{"Simple GPU", "gpu:2", 2.0},
		{"GPU with type", "gpu:tesla:4", 4.0},
		{"GPU with index", "gpu:1(IDX:0)", 1.0},
		{"Multiple GPUs", "gpu:2,gpu:tesla:1", 3.0},
		{"Empty string", "", 0.0},
		{"N/A", "N/A", 0.0},
		{"Null", "(null)", 0.0},
		{"No GPU", "cpu:8", 0.0},
		{"GPU uppercase", "GPU:3", 3.0},
		{"Complex GRES", "gpu:a100:8(IDX:0-7)", 8.0},
		{"Mixed resources", "gpu:2,mem:10G", 2.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseGresGpuCount(tt.gres)
			assert.Equal(t, tt.expected, result, "Failed for input: %s", tt.gres)
		})
	}
}

func TestNewGpuCollector(t *testing.T) {
	assert := assert.New(t)

	// Test with fallback mode
	config := &Config{
		PollLimit: 10.0,
		cliOpts: &CliOpts{
			fallback:    true,
			sinfoGpu:    []string{"sinfo", "-h", "-O", "Gres:30|"},
			sacctGpu:    []string{"sacct", "-a", "-X", "--format=AllocGRES"},
			gpusEnabled: true,
		},
	}
	collector := NewGpuCollector(config)
	assert.NotNil(collector)
	assert.IsType(&GpuCliFallbackFetcher{}, collector.fetcher)

	// Test with JSON mode
	config.cliOpts.fallback = false
	collector = NewGpuCollector(config)
	assert.NotNil(collector)
	assert.IsType(&GpuJsonFetcher{}, collector.fetcher)
}

func TestGpuJsonFetcher(t *testing.T) {
	assert := assert.New(t)

	fetcher := &GpuJsonFetcher{
		sinfoScraper: MockGpuSinfoScraper,
		sacctScraper: MockGpuSacctScraper,
		errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_gpu_errors",
		}),
		cache: &gpuCache{
			limit: 10.0,
		},
	}

	metrics, err := fetcher.FetchMetrics()
	assert.Nil(err)
	assert.NotNil(metrics)
	assert.GreaterOrEqual(metrics.Total, metrics.Alloc)
	assert.Equal(metrics.Idle, metrics.Total-metrics.Alloc)

	if metrics.Total > 0 {
		assert.Equal(metrics.Utilization, metrics.Alloc/metrics.Total)
	} else {
		assert.Equal(0.0, metrics.Utilization)
	}
}

func TestGpuCliFallbackFetcher(t *testing.T) {
	assert := assert.New(t)

	fetcher := &GpuCliFallbackFetcher{
		sinfoScraper: MockGpuSinfoFallbackScraper,
		sacctScraper: MockGpuSacctFallbackScraper,
		errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_gpu_errors",
		}),
		cache: &gpuCache{
			limit: 10.0,
		},
	}

	metrics, err := fetcher.FetchMetrics()
	assert.Nil(err)
	assert.NotNil(metrics)
	assert.GreaterOrEqual(metrics.Total, 0.0)
	assert.GreaterOrEqual(metrics.Alloc, 0.0)
	assert.Equal(metrics.Idle, metrics.Total-metrics.Alloc)
}

func TestGpuMetricsCache(t *testing.T) {
	assert := assert.New(t)

	fetcher := &GpuJsonFetcher{
		sinfoScraper: MockGpuSinfoScraper,
		sacctScraper: MockGpuSacctScraper,
		errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_gpu_errors",
		}),
		cache: &gpuCache{
			limit: 10.0,
		},
	}

	// First fetch
	metrics1, err := fetcher.FetchMetrics()
	assert.Nil(err)
	assert.NotNil(metrics1)

	// Second fetch should use cache
	metrics2, err := fetcher.FetchMetrics()
	assert.Nil(err)
	assert.NotNil(metrics2)

	// Metrics should be identical due to caching
	assert.Equal(metrics1.Total, metrics2.Total)
	assert.Equal(metrics1.Alloc, metrics2.Alloc)
	assert.Equal(metrics1.Idle, metrics2.Idle)
	assert.Equal(metrics1.Utilization, metrics2.Utilization)
}

func TestGpuCollectorCollect(t *testing.T) {
	assert := assert.New(t)

	config := &Config{
		PollLimit: 10.0,
		cliOpts: &CliOpts{
			fallback:    false,
			sinfoGpu:    []string{"sinfo", "--json"},
			sacctGpu:    []string{"sacct", "--json"},
			gpusEnabled: true,
		},
	}

	// Create a custom fetcher for testing
	collector := NewGpuCollector(config)
	collector.fetcher = &GpuJsonFetcher{
		sinfoScraper: MockGpuSinfoScraper,
		sacctScraper: MockGpuSacctScraper,
		errorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_gpu_errors",
		}),
		cache: &gpuCache{
			limit: 10.0,
		},
	}

	ch := make(chan prometheus.Metric, 10)
	collector.Collect(ch)
	close(ch)

	metricCount := 0
	for range ch {
		metricCount++
	}

	// Should collect 4 metrics: alloc, idle, total, utilization
	assert.Equal(4, metricCount)
}

func TestGpuCollectorDescribe(t *testing.T) {
	assert := assert.New(t)

	config := &Config{
		PollLimit: 10.0,
		cliOpts: &CliOpts{
			fallback:    false,
			gpusEnabled: true,
		},
	}

	collector := NewGpuCollector(config)

	ch := make(chan *prometheus.Desc, 10)
	collector.Describe(ch)
	close(ch)

	descCount := 0
	for range ch {
		descCount++
	}

	// Should describe 4 metrics
	assert.Equal(4, descCount)
}
