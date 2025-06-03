package controller

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// DownloadSession represents a single download session for a bucket/object
type DownloadSession struct {
	Bucket                       string
	Object                       string
	HeadObjectHandlerLatency     time.Duration
	GetObjectHandlerLatency      time.Duration
	GetObjectWithFileInfoLatency time.Duration
	ErasureDecodeLatencies       []time.Duration
}

// DownloadLatencyManager manages download latency data
type DownloadLatencyManager struct {
	mu sync.RWMutex

	// Map with bucket/object as key for efficient access
	downloadSessions map[string]*DownloadSession
}

// NewDownloadLatencyManager creates a new DownloadLatencyManager
func NewDownloadLatencyManager() *DownloadLatencyManager {
	return &DownloadLatencyManager{
		downloadSessions: make(map[string]*DownloadSession),
	}
}

// RecordGetObjectFileInfoLatency records the latency of getting object file info
func (m *DownloadLatencyManager) RecordGetObjectFileInfoLatency(bucket, object, caller string, latency time.Duration) {
	if bucket == "" || object == "" {
		klog.Warningf("[YBS] Empty bucket or object provided for getObjectFileInfo latency record, ignoring - Bucket: %s, Object: %s",
			bucket, object)
		return
	}

	if caller == "" {
		klog.Warningf("[YBS] Empty caller provided for getObjectFileInfo latency record, ignoring - Bucket: %s, Object: %s",
			bucket, object)
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for getObjectFileInfo - Bucket: %s, Object: %s, Caller: %s, ignoring",
			latency, bucket, object, caller)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := bucket + "/" + object

	// Initialize session if not exists
	if m.downloadSessions[key] == nil {
		m.downloadSessions[key] = &DownloadSession{
			Bucket:                 bucket,
			Object:                 object,
			ErasureDecodeLatencies: make([]time.Duration, 0),
		}
	}

	// Update latency based on caller
	switch caller {
	case "headObjectHandler":
		m.downloadSessions[key].HeadObjectHandlerLatency = latency
		klog.Infof("[YBS] Recorded headObjectHandler latency - Bucket: %s, Object: %s, Latency: %v",
			bucket, object, latency)
	case "getObjectHandler":
		m.downloadSessions[key].GetObjectHandlerLatency = latency
		klog.Infof("[YBS] Recorded getObjectHandler latency - Bucket: %s, Object: %s, Latency: %v",
			bucket, object, latency)
	default:
		klog.Warningf("[YBS] Unknown caller '%s' for getObjectFileInfo - Bucket: %s, Object: %s, ignoring",
			caller, bucket, object)
	}
}

// RecordGetObjectWithFileInfoLatency records the latency of getting object with file info
func (m *DownloadLatencyManager) RecordGetObjectWithFileInfoLatency(bucket, object string, latency time.Duration) {
	if bucket == "" || object == "" {
		klog.Warningf("[YBS] Empty bucket or object provided for getObjectWithFileInfo latency record, ignoring - Bucket: %s, Object: %s",
			bucket, object)
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for getObjectWithFileInfo - Bucket: %s, Object: %s, ignoring",
			latency, bucket, object)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := bucket + "/" + object

	// Try to find existing session, wait if necessary
	for i := range 5 {
		session, exists := m.downloadSessions[key]
		if exists && session != nil {
			session.GetObjectWithFileInfoLatency = latency
			klog.Infof("[YBS] Recorded getObjectWithFileInfo latency - Bucket: %s, Object: %s, Latency: %v",
				bucket, object, latency)
			return
		}

		klog.Infof("[YBS] Download session not found for %s, waiting briefly... (attempt %d/5)",
			key, i+1)

		// 뮤텍스 락을 풀고 대기 (다른 고루틴이 접근할 수 있도록)
		m.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock() // 다시 락 획득
	}

	// If still not found after 5 attempts, create new session
	klog.Warningf("[YBS] Failed to find download session after 5 attempts, creating new session - Bucket: %s, Object: %s",
		bucket, object)

	m.downloadSessions[key] = &DownloadSession{
		Bucket:                       bucket,
		Object:                       object,
		GetObjectWithFileInfoLatency: latency,
		ErasureDecodeLatencies:       make([]time.Duration, 0),
	}

	klog.Infof("[YBS] Created new download session and recorded getObjectWithFileInfo latency - Bucket: %s, Object: %s, Latency: %v",
		bucket, object, latency)
}

// RecordErasureDecodeEachPartLatency records the latency of erasure decode each part
func (m *DownloadLatencyManager) RecordErasureDecodeEachPartLatency(bucket, object string, partIndex int, latency time.Duration) {
	if bucket == "" || object == "" {
		klog.Warningf("[YBS] Empty bucket or object provided for erasureDecodeEachPart latency record, ignoring - Bucket: %s, Object: %s",
			bucket, object)
		return
	}

	if partIndex < 0 {
		klog.Warningf("[YBS] Invalid partIndex (%d) for erasureDecodeEachPart - Bucket: %s, Object: %s, ignoring",
			partIndex, bucket, object)
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for erasureDecodeEachPart - Bucket: %s, Object: %s, PartIndex: %d, ignoring",
			latency, bucket, object, partIndex)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := bucket + "/" + object

	// Try to find existing session, wait if necessary
	for i := range 5 {
		session, exists := m.downloadSessions[key]
		if exists && session != nil {
			session.ErasureDecodeLatencies = append(session.ErasureDecodeLatencies, latency)
			klog.Infof("[YBS] Recorded erasureDecodeEachPart latency - Bucket: %s, Object: %s, PartIndex: %d, Latency: %v, Total parts: %d",
				bucket, object, partIndex, latency, len(session.ErasureDecodeLatencies))
			return
		}

		klog.Infof("[YBS] Download session not found for %s, waiting briefly... (attempt %d/5)",
			key, i+1)

		// 뮤텍스 락을 풀고 대기 (다른 고루틴이 접근할 수 있도록)
		m.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock() // 다시 락 획득
	}

	// If still not found after 5 attempts, create new session
	klog.Warningf("[YBS] Failed to find download session after 5 attempts, creating new session - Bucket: %s, Object: %s",
		bucket, object)

	m.downloadSessions[key] = &DownloadSession{
		Bucket:                 bucket,
		Object:                 object,
		ErasureDecodeLatencies: []time.Duration{latency},
	}

	klog.Infof("[YBS] Created new download session and recorded erasureDecodeEachPart latency - Bucket: %s, Object: %s, PartIndex: %d, Latency: %v",
		bucket, object, partIndex, latency)
}

// GetLatencyStats returns download latency statistics for multiple files in the desired JSON format
func (m *DownloadLatencyManager) GetLatencyStats() (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Filter out sessions with bucket names containing dots (e.g., .minio.sys)
	validSessions := make(map[string]*DownloadSession)
	for key, sess := range m.downloadSessions {
		if !strings.Contains(sess.Bucket, ".") {
			validSessions[key] = sess
		}
	}

	// If no valid sessions, return empty arrays
	if len(validSessions) == 0 {
		return map[string]any{
			"downloadLatencies": []any{},
			"aggregatedStats":   nil,
		}, nil
	}

	// Build individual file statistics
	var downloadLatencies []any
	var fileStats []map[string]any

	for key, session := range validSessions {
		individualStats := map[string]any{
			"bucketObject": session.Bucket + "/" + session.Object,
			"metadata": map[string]any{
				"headObjectHandler": map[string]any{
					"latency": m.formatDuration(session.HeadObjectHandlerLatency),
				},
				"getObjectHandler": map[string]any{
					"latency": m.formatDuration(session.GetObjectHandlerLatency),
				},
				"total": m.formatDuration(session.HeadObjectHandlerLatency + session.GetObjectHandlerLatency),
			},
			"dataTransfer": map[string]any{
				"getObjectWithFileInfo": map[string]any{
					"latency": m.formatDuration(session.GetObjectWithFileInfoLatency),
				},
				"erasureDecode": m.calculateErasureDecodeStats(session.ErasureDecodeLatencies),
			},
		}

		downloadLatencies = append(downloadLatencies, individualStats)

		// Store raw stats for aggregation
		fileStats = append(fileStats, map[string]any{
			"headObjectHandler":     session.HeadObjectHandlerLatency,
			"getObjectHandler":      session.GetObjectHandlerLatency,
			"metadataTotal":         session.HeadObjectHandlerLatency + session.GetObjectHandlerLatency,
			"getObjectWithFileInfo": session.GetObjectWithFileInfoLatency,
			"erasureDecodeStats":    m.calculateErasureDecodeStats(session.ErasureDecodeLatencies),
		})

		klog.V(4).Infof("[YBS] Processing download session: %s (filtered, excluding system buckets)", key)
	}

	// Calculate aggregated statistics
	aggregatedStats := m.calculateAggregatedStats(fileStats)

	return map[string]any{
		"downloadLatencies": downloadLatencies,
		"aggregatedStats":   aggregatedStats,
	}, nil
}

// calculateAggregatedStats calculates aggregated statistics from multiple file stats
func (m *DownloadLatencyManager) calculateAggregatedStats(fileStats []map[string]any) map[string]any {
	if len(fileStats) == 0 {
		return nil
	}

	// Collect latency values for each metric
	var headObjectLatencies, getObjectLatencies, metadataTotals, getObjectWithFileInfoLatencies []time.Duration
	var erasureDecodeStats []map[string]any

	for _, stats := range fileStats {
		headObjectLatencies = append(headObjectLatencies, stats["headObjectHandler"].(time.Duration))
		getObjectLatencies = append(getObjectLatencies, stats["getObjectHandler"].(time.Duration))
		metadataTotals = append(metadataTotals, stats["metadataTotal"].(time.Duration))
		getObjectWithFileInfoLatencies = append(getObjectWithFileInfoLatencies, stats["getObjectWithFileInfo"].(time.Duration))
		erasureDecodeStats = append(erasureDecodeStats, stats["erasureDecodeStats"].(map[string]any))
	}

	// Calculate aggregated metadata stats
	headObjectAgg := m.calculateLatencyAggregation(headObjectLatencies)
	getObjectAgg := m.calculateLatencyAggregation(getObjectLatencies)
	metadataTotalAgg := m.calculateLatencyAggregation(metadataTotals)

	// Calculate aggregated data transfer stats
	getObjectWithFileInfoAgg := m.calculateLatencyAggregation(getObjectWithFileInfoLatencies)
	erasureDecodeAgg := m.calculateErasureDecodeAggregation(erasureDecodeStats)

	return map[string]any{
		"metadata": map[string]any{
			"headObjectHandler": map[string]any{
				"averageLatency":       headObjectAgg["average"],
				"standardDeviation":    headObjectAgg["standardDeviation"],
				"confidenceInterval95": headObjectAgg["confidenceInterval95"],
			},
			"getObjectHandler": map[string]any{
				"averageLatency":       getObjectAgg["average"],
				"standardDeviation":    getObjectAgg["standardDeviation"],
				"confidenceInterval95": getObjectAgg["confidenceInterval95"],
			},
			"total": map[string]any{
				"averageLatency":       metadataTotalAgg["average"],
				"standardDeviation":    metadataTotalAgg["standardDeviation"],
				"confidenceInterval95": metadataTotalAgg["confidenceInterval95"],
			},
		},
		"dataTransfer": map[string]any{
			"getObjectWithFileInfo": map[string]any{
				"averageLatency":       getObjectWithFileInfoAgg["average"],
				"standardDeviation":    getObjectWithFileInfoAgg["standardDeviation"],
				"confidenceInterval95": getObjectWithFileInfoAgg["confidenceInterval95"],
			},
			"erasureDecode": erasureDecodeAgg,
		},
	}
}

// calculateLatencyAggregation calculates average, standard deviation, and 95% confidence interval
func (m *DownloadLatencyManager) calculateLatencyAggregation(latencies []time.Duration) map[string]any {
	if len(latencies) == 0 {
		return map[string]any{
			"average":              "0ms",
			"standardDeviation":    "0ms",
			"confidenceInterval95": map[string]any{"lower": "0ms", "upper": "0ms"},
		}
	}

	// Calculate average
	var total time.Duration
	for _, latency := range latencies {
		total += latency
	}
	average := total / time.Duration(len(latencies))

	// Calculate standard deviation
	var sumSquaredDiff float64
	for _, latency := range latencies {
		diff := float64(latency.Nanoseconds()) - float64(average.Nanoseconds())
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(len(latencies))
	standardDeviation := time.Duration(math.Sqrt(variance))

	// Calculate 95% confidence interval
	standardError := standardDeviation / time.Duration(math.Sqrt(float64(len(latencies))))
	marginOfError := time.Duration(1.96 * float64(standardError.Nanoseconds()))
	ci95Lower := average - marginOfError
	ci95Upper := average + marginOfError

	if ci95Lower < 0 {
		ci95Lower = 0
	}

	return map[string]any{
		"average":           m.formatDuration(average),
		"standardDeviation": m.formatDuration(standardDeviation),
		"confidenceInterval95": map[string]any{
			"lower": m.formatDuration(ci95Lower),
			"upper": m.formatDuration(ci95Upper),
		},
	}
}

// calculateErasureDecodeAggregation calculates aggregated erasure decode statistics
func (m *DownloadLatencyManager) calculateErasureDecodeAggregation(erasureStats []map[string]any) map[string]any {
	if len(erasureStats) == 0 {
		return map[string]any{
			"averageLatency":       "0ms",
			"standardDeviation":    "0ms",
			"confidenceInterval95": map[string]any{"lower": "0ms", "upper": "0ms"},
			"totalParts":           0,
		}
	}

	// Extract average latencies from each file's erasure decode stats
	var averageLatencies []time.Duration
	totalParts := 0

	for _, stats := range erasureStats {
		if parts, ok := stats["totalParts"].(int); ok {
			totalParts += parts
		}

		// Parse average latency string back to duration for aggregation
		if avgLatencyStr, ok := stats["averageLatency"].(string); ok {
			if avgLatency := m.parseDurationFromString(avgLatencyStr); avgLatency > 0 {
				averageLatencies = append(averageLatencies, avgLatency)
			}
		}
	}

	// Calculate aggregation of average latencies
	aggregation := m.calculateLatencyAggregation(averageLatencies)

	return map[string]any{
		"averageLatency":       aggregation["average"],
		"standardDeviation":    aggregation["standardDeviation"],
		"confidenceInterval95": aggregation["confidenceInterval95"],
		"totalParts":           totalParts,
	}
}

// parseDurationFromString parses a duration string back to time.Duration
func (m *DownloadLatencyManager) parseDurationFromString(s string) time.Duration {
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	return 0
}

// calculateErasureDecodeStats calculates statistics for erasure decode latencies
func (m *DownloadLatencyManager) calculateErasureDecodeStats(latencies []time.Duration) map[string]any {
	if len(latencies) == 0 {
		return map[string]any{
			"totalParts":        0,
			"averageLatency":    "0ms",
			"minLatency":        "0ms",
			"maxLatency":        "0ms",
			"standardDeviation": "0ms",
			"confidenceInterval95": map[string]any{
				"lower": "0ms",
				"upper": "0ms",
			},
			"totalDecodeTime": "0ms",
		}
	}

	// Calculate basic statistics
	var total time.Duration
	minLatency := latencies[0]
	maxLatency := latencies[0]

	for _, latency := range latencies {
		total += latency
		if latency < minLatency {
			minLatency = latency
		}
		if latency > maxLatency {
			maxLatency = latency
		}
	}

	average := total / time.Duration(len(latencies))

	// Calculate standard deviation
	var sumSquaredDiff float64
	for _, latency := range latencies {
		diff := float64(latency.Nanoseconds()) - float64(average.Nanoseconds())
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(len(latencies))
	standardDeviation := time.Duration(math.Sqrt(variance))

	// Calculate 95% confidence interval
	standardError := standardDeviation / time.Duration(math.Sqrt(float64(len(latencies))))
	marginOfError := time.Duration(1.96 * float64(standardError.Nanoseconds()))
	ci95Lower := average - marginOfError
	ci95Upper := average + marginOfError

	if ci95Lower < 0 {
		ci95Lower = 0
	}

	return map[string]any{
		"totalParts":        len(latencies),
		"averageLatency":    m.formatDuration(average),
		"minLatency":        m.formatDuration(minLatency),
		"maxLatency":        m.formatDuration(maxLatency),
		"standardDeviation": m.formatDuration(standardDeviation),
		"confidenceInterval95": map[string]any{
			"lower": m.formatDuration(ci95Lower),
			"upper": m.formatDuration(ci95Upper),
		},
		"totalDecodeTime": m.formatDuration(total),
	}
}

// formatDuration formats a duration to a string with ms unit
func (m *DownloadLatencyManager) formatDuration(d time.Duration) string {
	if d == 0 {
		return "0ms"
	}

	ms := float64(d.Nanoseconds()) / 1e6
	if ms < 1 {
		return "0ms"
	}

	// Always return in ms unit with 3 decimal places for precision
	return fmt.Sprintf("%.3fms", ms)
}

// ClearAllMetrics clears all download latency metrics
func (m *DownloadLatencyManager) ClearAllMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	sessionCount := len(m.downloadSessions)
	m.downloadSessions = make(map[string]*DownloadSession)

	klog.Infof("[YBS] Cleared all download latency metrics - Sessions cleared: %d", sessionCount)
}
