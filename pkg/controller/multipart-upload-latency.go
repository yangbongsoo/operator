package controller

import (
	"sort"
	"sync"
	"time"

	"maps"

	"k8s.io/klog/v2"
)

// UploadLatencyManager는 멀티파트 업로드 지연 시간 데이터를 관리합니다.
type UploadLatencyManager struct {
	mu sync.RWMutex

	multipartUploadMetric map[string]*MultipartUploadMetric
}

// MultipartUploadMetric는 하나의 멀티파트 업로드에 관한 모든 정보를 저장합니다.
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

// PartInfo는 하나의 업로드 파트에 관한 정보를 저장합니다.
type PartInfo struct {
	PartID                int           // 파트 ID
	EachPartUploadLatency time.Duration // 파트 업로드 지연 시간
}

func NewUploadLatencyManager() *UploadLatencyManager {
	return &UploadLatencyManager{
		multipartUploadMetric: make(map[string]*MultipartUploadMetric),
	}
}

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

// RecordPartUpload는 파트 업로드를 기록합니다.
func (m *UploadLatencyManager) RecordPartUpload(uploadID, bucket, object string, partID int, eachPartUploadLatency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range 5 {
		info, exists := m.multipartUploadMetric[uploadID]
		if exists {
			info.Parts[partID] = &PartInfo{
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

func (m *UploadLatencyManager) RecordUploadComplete(uploadID, bucket, object string, completeTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i := range 5 {
		info, exists := m.multipartUploadMetric[uploadID]
		if exists {
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

func (m *UploadLatencyManager) GetMultipartUploadMetric(uploadID string) *MultipartUploadMetric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	multipartUploadMetric := m.multipartUploadMetric[uploadID]
	if multipartUploadMetric.IsComplete {
		return multipartUploadMetric
	}

	return nil
}

func (m *UploadLatencyManager) GetAllMultipartUploadMetric() map[string]*MultipartUploadMetric {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// 맵 복사본 생성
	result := make(map[string]*MultipartUploadMetric, len(m.multipartUploadMetric))
	maps.Copy(result, m.multipartUploadMetric)

	return result
}

func (m *UploadLatencyManager) GetLatencyStats() (map[string]any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := make(map[string]any)

	// 1. TotalLatency 통계 계산
	var totalLatencySum time.Duration
	var totalLatencyMin time.Duration
	var totalLatencyMax time.Duration
	completedUploadCount := 0

	// 2. EachPartUploadLatency 통계 계산
	var partLatencySum time.Duration
	var partLatencyMin time.Duration
	var partLatencyMax time.Duration
	totalPartCount := 0

	// 3. part 별 latency
	partDetails := make(map[string][]map[string]any)

	// 초기값 설정
	isFirst := true
	isFirstPart := true

	// 모든 업로드를 순회하며 통계 계산
	for _, uploadMetric := range m.multipartUploadMetric {
		// 완료된 업로드만 계산에 포함
		if uploadMetric.IsComplete {
			// TotalLatency 통계
			if isFirst || uploadMetric.TotalLatency < totalLatencyMin {
				totalLatencyMin = uploadMetric.TotalLatency
			}

			if isFirst || uploadMetric.TotalLatency > totalLatencyMax {
				totalLatencyMax = uploadMetric.TotalLatency
			}

			totalLatencySum += uploadMetric.TotalLatency
			completedUploadCount++
			isFirst = false

			// 완료된 업로드의 파트에 대한 통계 계산
			for _, part := range uploadMetric.Parts {
				if isFirstPart || part.EachPartUploadLatency < partLatencyMin {
					partLatencyMin = part.EachPartUploadLatency
				}

				if isFirstPart || part.EachPartUploadLatency > partLatencyMax {
					partLatencyMax = part.EachPartUploadLatency
				}

				partLatencySum += part.EachPartUploadLatency
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
					"partID":  part.PartID,
					"latency": part.EachPartUploadLatency,
				})
			}
			partDetails[uploadMetric.UploadID] = parts
		}
	}

	// TotalLatency 통계 결과
	totalLatencyStats := make(map[string]any)
	if completedUploadCount > 0 {
		totalLatencyStats["average"] = totalLatencySum / time.Duration(completedUploadCount)
		totalLatencyStats["min"] = totalLatencyMin
		totalLatencyStats["max"] = totalLatencyMax
		totalLatencyStats["count"] = completedUploadCount
	} else {
		totalLatencyStats["average"] = time.Duration(0)
		totalLatencyStats["min"] = time.Duration(0)
		totalLatencyStats["max"] = time.Duration(0)
		totalLatencyStats["count"] = 0
	}

	// EachPartUploadLatency 통계 결과
	partLatencyStats := make(map[string]any)
	if totalPartCount > 0 {
		partLatencyStats["average"] = partLatencySum / time.Duration(totalPartCount)
		partLatencyStats["min"] = partLatencyMin
		partLatencyStats["max"] = partLatencyMax
		partLatencyStats["count"] = totalPartCount
	} else {
		partLatencyStats["average"] = time.Duration(0)
		partLatencyStats["min"] = time.Duration(0)
		partLatencyStats["max"] = time.Duration(0)
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
