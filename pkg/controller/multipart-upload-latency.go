package controller

import (
	"maps"
	"math"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// UploadLatencyManager manages multipart upload latency data.
type UploadLatencyManager struct {
	mu sync.RWMutex

	multipartUploadMetric map[string]*MultipartUploadMetric
}

// MultipartUploadMetric is all information about a multipart upload.
type MultipartUploadMetric struct {
	UploadID     string            // 업로드 ID
	Bucket       string            // 버킷 이름
	Object       string            // 객체 이름
	StartTime    time.Time         // 업로드 시작 시간
	CompleteTime time.Time         // 업로드 완료 시간
	TotalLatency time.Duration     // 업로드 총 지연 시간
	Parts        map[int]*PartInfo // 파트 ID -> 파트 정보 매핑
	IsComplete   bool              // 업로드 완료 여부
}

// PartInfo is all information about a part of a multipart upload.
type PartInfo struct {
	Bucket                string        // 버킷 이름
	Object                string        // 객체 이름
	PartID                int           // 파트 ID
	EachPartUploadLatency time.Duration // 파트 업로드 지연 시간
}

// NewUploadLatencyManager creates a new UploadLatencyManager.
func NewUploadLatencyManager() *UploadLatencyManager {
	return &UploadLatencyManager{
		multipartUploadMetric: make(map[string]*MultipartUploadMetric),
	}
}

// RecordUploadStart records the start of a multipart upload.
func (m *UploadLatencyManager) RecordUploadStart(uploadID, bucket, object string, startTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.multipartUploadMetric[uploadID]; exists {
		klog.Infof("[YBS] Upload ID already exists: %s", uploadID)
		return
	}

	m.multipartUploadMetric[uploadID] = &MultipartUploadMetric{
		UploadID:   uploadID,
		Bucket:     bucket,
		Object:     object,
		StartTime:  startTime,
		Parts:      make(map[int]*PartInfo),
		IsComplete: false,
	}

	klog.Infof("[YBS] Recorded upload start - ID: %s, Bucket: %s, Object: %s", uploadID, bucket, object)
}

// RecordPartUpload records the part upload.
func (m *UploadLatencyManager) RecordPartUpload(uploadID, bucket, object string, partID int, eachPartUploadLatency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range 5 {
		info, exists := m.multipartUploadMetric[uploadID]
		if exists {
			info.Parts[partID] = &PartInfo{
				Bucket:                bucket,
				Object:                object,
				PartID:                partID,
				EachPartUploadLatency: eachPartUploadLatency,
			}

			klog.Infof("[YBS] Recorded part upload - ID: %s, Part: %d, eachPartUploadLatency: %v",
				uploadID, partID, eachPartUploadLatency)
			return
		}

		klog.Infof("[YBS] Upload info not found for ID: %s, waiting briefly... (attempt %d/5)",
			uploadID, i+1)

		// 뮤텍스 락을 풀고 대기 (다른 고루틴이 접근할 수 있도록)
		m.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock() // 다시 락 획득
	}

	klog.Warningf("[YBS] Failed to find upload info after 5 attempts - ID: %s, Part: %d",
		uploadID, partID)
}

// RecordUploadComplete records the completion of a multipart upload.
func (m *UploadLatencyManager) RecordUploadComplete(uploadID, bucket, object string, completeTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range 5 {
		info, exists := m.multipartUploadMetric[uploadID]
		if exists {
			if info.Bucket != bucket || info.Object != object {
				klog.Warningf("[YBS] Upload info mismatch - ID: %s, Bucket: %s, Object: %s",
					uploadID, info.Bucket, info.Object)
				return
			}

			// 업로드 정보가 존재하면 완료 상태 업데이트하고 종료
			info.IsComplete = true
			info.CompleteTime = completeTime
			info.TotalLatency = info.CompleteTime.Sub(info.StartTime)
			klog.Infof("[YBS] Completed upload - ID: %s, Bucket: %s, Object: %s, Parts: %d, Duration: %v",
				uploadID, info.Bucket, info.Object, len(info.Parts), info.CompleteTime.Sub(info.StartTime))
			return
		}

		klog.Infof("[YBS] Upload info not found for ID: %s, waiting briefly... (attempt %d/5)",
			uploadID, i+1)

		// 뮤텍스 락을 풀고 대기 (다른 고루틴이 접근할 수 있도록)
		m.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock() // 다시 락 획득
	}

	klog.Warningf("[YBS] Failed to find upload info after 5 attempts - ID: %s", uploadID)
}

// GetMultipartUploadMetric gets the multipart upload metric.
func (m *UploadLatencyManager) GetMultipartUploadMetric(uploadID string) *MultipartUploadMetric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	multipartUploadMetric := m.multipartUploadMetric[uploadID]
	if multipartUploadMetric.IsComplete {
		return multipartUploadMetric
	}

	return nil
}

// GetAllMultipartUploadMetric gets all multipart upload metrics.
func (m *UploadLatencyManager) GetAllMultipartUploadMetric() map[string]*MultipartUploadMetric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 맵 복사본 생성
	result := make(map[string]*MultipartUploadMetric, len(m.multipartUploadMetric))
	maps.Copy(result, m.multipartUploadMetric)

	return result
}

// GetLatencyStats gets the latency stats.
func (m *UploadLatencyManager) GetLatencyStats() (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]any)

	// 1. TotalLatency 통계 계산
	var sumTotalLatency time.Duration
	var minTotalLatency time.Duration
	var maxTotalLatency time.Duration
	completedUploadCount := 0

	// 2. EachPartUploadLatency 통계 계산
	var sumPartLatency time.Duration
	var minPartLatency time.Duration
	var maxPartLatency time.Duration
	totalPartCount := 0

	// 3. part 별 latency
	partDetails := make(map[string][]map[string]any)

	// 초기값 설정
	isFirst := true
	isFirstPart := true

	// 표준편차 계산을 위한 제곱합
	var sumSquaredTotalLatencyMS float64
	var sumSquaredPartLatencyMS float64

	// 모든 업로드를 순회하며 통계 계산
	for _, uploadMetric := range m.multipartUploadMetric {
		// 완료된 업로드만 계산에 포함
		if uploadMetric.IsComplete {
			// TotalLatency 통계
			if isFirst || uploadMetric.TotalLatency < minTotalLatency {
				minTotalLatency = uploadMetric.TotalLatency
			}

			if isFirst || uploadMetric.TotalLatency > maxTotalLatency {
				maxTotalLatency = uploadMetric.TotalLatency
			}

			sumTotalLatency += uploadMetric.TotalLatency

			// 표준편차 계산을 위한 제곱합 (밀리초 단위)
			latencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
			sumSquaredTotalLatencyMS += latencyMS * latencyMS

			completedUploadCount++
			isFirst = false

			// 완료된 업로드의 파트에 대한 통계 계산
			for _, part := range uploadMetric.Parts {
				if isFirstPart || part.EachPartUploadLatency < minPartLatency {
					minPartLatency = part.EachPartUploadLatency
				}

				if isFirstPart || part.EachPartUploadLatency > maxPartLatency {
					maxPartLatency = part.EachPartUploadLatency
				}

				sumPartLatency += part.EachPartUploadLatency

				// 파트 지연시간 제곱합 (밀리초 단위)
				partLatencyMS := float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6
				sumSquaredPartLatencyMS += partLatencyMS * partLatencyMS

				totalPartCount++
				isFirstPart = false
			}

			// part
			parts := make([]map[string]any, 0, len(uploadMetric.Parts))
			partIDs := make([]int, 0, len(uploadMetric.Parts))
			for partID := range uploadMetric.Parts {
				partIDs = append(partIDs, partID)
			}
			sort.Ints(partIDs)

			for _, partID := range partIDs {
				part := uploadMetric.Parts[partID]
				parts = append(parts, map[string]any{
					"partID":    part.PartID,
					"latencyMS": float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6,
				})
			}
			partDetails[uploadMetric.UploadID] = parts
		}
	}

	// TotalLatency 통계 결과
	totalLatencyStats := make(map[string]any)
	if completedUploadCount > 0 {
		avgTotalLatency := sumTotalLatency / time.Duration(completedUploadCount)
		avgTotalLatencyMS := float64(avgTotalLatency.Nanoseconds()) / 1e6

		// 분산 계산
		varianceMS := (sumSquaredTotalLatencyMS / float64(completedUploadCount)) - (avgTotalLatencyMS * avgTotalLatencyMS)
		if varianceMS < 0 {
			// 수치적 오류로 음수가 나올 경우 0으로 처리
			varianceMS = 0
		}
		// 표준편차 계산
		stdDevMS := math.Sqrt(varianceMS)

		// 95% 신뢰구간 계산
		standardErrorMS := stdDevMS / math.Sqrt(float64(completedUploadCount))
		ci95LowerMS := avgTotalLatencyMS - 1.96*standardErrorMS
		ci95UpperMS := avgTotalLatencyMS + 1.96*standardErrorMS

		// 밀리초 단위만 유지
		totalLatencyStats["averageMS"] = avgTotalLatencyMS
		totalLatencyStats["minMS"] = float64(minTotalLatency.Nanoseconds()) / 1e6
		totalLatencyStats["maxMS"] = float64(maxTotalLatency.Nanoseconds()) / 1e6
		totalLatencyStats["stdDevMS"] = stdDevMS
		totalLatencyStats["ci95LowerMS"] = ci95LowerMS
		totalLatencyStats["ci95UpperMS"] = ci95UpperMS
		totalLatencyStats["count"] = completedUploadCount
	} else {
		// 밀리초 단위만 유지
		totalLatencyStats["averageMS"] = float64(0)
		totalLatencyStats["minMS"] = float64(0)
		totalLatencyStats["maxMS"] = float64(0)
		totalLatencyStats["stdDevMS"] = float64(0)
		totalLatencyStats["ci95LowerMS"] = float64(0)
		totalLatencyStats["ci95UpperMS"] = float64(0)
		totalLatencyStats["count"] = 0
	}

	// EachPartUploadLatency 통계 결과
	partLatencyStats := make(map[string]any)
	if totalPartCount > 0 {
		avgPartLatency := sumPartLatency / time.Duration(totalPartCount)
		avgPartLatencyMS := float64(avgPartLatency.Nanoseconds()) / 1e6

		// 분산 계산
		partVarianceMS := (sumSquaredPartLatencyMS / float64(totalPartCount)) - (avgPartLatencyMS * avgPartLatencyMS)
		if partVarianceMS < 0 {
			// 수치적 오류로 음수가 나올 경우 0으로 처리
			partVarianceMS = 0
		}
		// 표준편차 계산
		partStdDevMS := math.Sqrt(partVarianceMS)

		// 95% 신뢰구간 계산
		partStandardErrorMS := partStdDevMS / math.Sqrt(float64(totalPartCount))
		partCi95LowerMS := avgPartLatencyMS - 1.96*partStandardErrorMS
		partCi95UpperMS := avgPartLatencyMS + 1.96*partStandardErrorMS

		// 밀리초 단위만 유지
		partLatencyStats["averageMS"] = avgPartLatencyMS
		partLatencyStats["minMS"] = float64(minPartLatency.Nanoseconds()) / 1e6
		partLatencyStats["maxMS"] = float64(maxPartLatency.Nanoseconds()) / 1e6
		partLatencyStats["stdDevMS"] = partStdDevMS
		partLatencyStats["ci95LowerMS"] = partCi95LowerMS
		partLatencyStats["ci95UpperMS"] = partCi95UpperMS
		partLatencyStats["count"] = totalPartCount
	} else {
		// 밀리초 단위만 유지
		partLatencyStats["averageMS"] = float64(0)
		partLatencyStats["minMS"] = float64(0)
		partLatencyStats["maxMS"] = float64(0)
		partLatencyStats["stdDevMS"] = float64(0)
		partLatencyStats["ci95LowerMS"] = float64(0)
		partLatencyStats["ci95UpperMS"] = float64(0)
		partLatencyStats["count"] = 0
	}

	// 결과 맵 구성
	stats["totalLatency"] = totalLatencyStats
	stats["partLatency"] = partLatencyStats

	// 추가 정보
	stats["multipartUploadMetricCount"] = len(m.multipartUploadMetric)
	stats["completedUploadCount"] = completedUploadCount
	stats["inProgressUploads"] = len(m.multipartUploadMetric) - completedUploadCount
	stats["partDetails"] = partDetails
	return stats, nil
}

// ClearMetricsAfterStats clears the metrics after getting the stats.
func (m *UploadLatencyManager) ClearMetricsAfterStats() {
	m.mu.Lock()
	defer m.mu.Unlock()

	deletedCount := 0
	for uploadID, uploadMetric := range m.multipartUploadMetric {
		if uploadMetric.IsComplete {
			delete(m.multipartUploadMetric, uploadID)
			deletedCount++
		}
	}

	klog.Infof("[YBS] Cleared %d completed upload metrics, %d in-progress uploads remain",
		deletedCount, len(m.multipartUploadMetric))
}

// ClearAllMetrics clears all metrics regardless of their completion status.
func (m *UploadLatencyManager) ClearAllMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.multipartUploadMetric = make(map[string]*MultipartUploadMetric)

	klog.Infof("[YBS] Cleared all %d upload metrics (both completed and in-progress)", len(m.multipartUploadMetric))
}
