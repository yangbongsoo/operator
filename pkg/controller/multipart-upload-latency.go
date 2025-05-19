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

	multipartUploadMetric      map[string]*MultipartUploadMetric
	getActiveInfoLatency       map[string][]time.Duration
	checkUploadIDExistsLatency map[string][]time.Duration
	readAllFileInfoLatency     map[string][]time.Duration
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
		multipartUploadMetric:      make(map[string]*MultipartUploadMetric),
		getActiveInfoLatency:       make(map[string][]time.Duration),
		checkUploadIDExistsLatency: make(map[string][]time.Duration),
		readAllFileInfoLatency:     make(map[string][]time.Duration),
	}
}

// RecordCheckUploadIDExistsLatency records the latency of checking upload id exists.
func (m *UploadLatencyManager) RecordCheckUploadIDExistsLatency(uploadID, bucket, object string, latency time.Duration) {
	if uploadID == "" {
		klog.Warningf("[YBS] Empty uploadID provided for checkUploadIDExists latency record, ignoring")
		return
	}

	if bucket == "" || object == "" {
		klog.Warningf("[YBS] Empty bucket or object provided for checkUploadIDExists latency record - UploadID: %s, ignoring",
			uploadID)
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for uploadID %s, ignoring", latency, uploadID)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkUploadIDExistsLatency[uploadID] = append(m.checkUploadIDExistsLatency[uploadID], latency)

	klog.Infof("[YBS] Recorded checkUploadID exists latency - UploadID: %s, Latency: %v, Total records: %d",
		uploadID, latency, len(m.checkUploadIDExistsLatency[uploadID]))
}

// RecordReadAllFileInfoLatency records the latency of reading all file info.
func (m *UploadLatencyManager) RecordReadAllFileInfoLatency(bucket, object string, latency time.Duration) {
	if bucket == "" || object == "" {
		klog.Warningf("[YBS] Empty bucket or object provided for readAllFileInfo latency record, ignoring - Bucket: %s, Object: %s",
			bucket, object)
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for readAllFileInfo - Bucket: %s, Object: %s, ignoring",
			latency, bucket, object)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	key := bucket + "/" + object

	m.readAllFileInfoLatency[key] = append(m.readAllFileInfoLatency[key], latency)
	klog.Infof("[YBS] Recorded read all file info latency - Bucket: %s, Object: %s, Latency: %v, Total records: %d",
		bucket, object, latency, len(m.readAllFileInfoLatency[key]))
}

// RecordGetActiveInfoLatency records the latency of getting active info.
func (m *UploadLatencyManager) RecordGetActiveInfoLatency(tag string, latency time.Duration) {
	if tag == "" {
		klog.Warningf("[YBS] Empty tag provided for active info latency record, ignoring")
		return
	}

	if latency <= 0 {
		klog.Warningf("[YBS] Invalid latency value (%v) for tag %s, ignoring", latency, tag)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.getActiveInfoLatency[tag] = append(m.getActiveInfoLatency[tag], latency)
	klog.Infof("[YBS] Recorded get active info latency - Tag: %s, Latency: %v, Total records: %d",
		tag, latency, len(m.getActiveInfoLatency[tag]))
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
	var avgTotalLatencyMS float64

	if completedUploadCount > 0 {
		avgTotalLatency := sumTotalLatency / time.Duration(completedUploadCount)
		avgTotalLatencyMS = float64(avgTotalLatency.Nanoseconds()) / 1e6

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
	var avgPartLatencyMS float64

	if totalPartCount > 0 {
		avgPartLatency := sumPartLatency / time.Duration(totalPartCount)
		avgPartLatencyMS = float64(avgPartLatency.Nanoseconds()) / 1e6

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

	// getActiveInfoLatency 통계 계산
	activeInfoLatencyStats := make(map[string]any)
	tagLatencyStats := make(map[string]map[string]any)
	var totalActiveInfoLatency time.Duration
	totalActiveInfoCount := 0

	// 각 태그별 지연 시간 합계 및 통계 계산
	for tag, latencies := range m.getActiveInfoLatency {
		if len(latencies) == 0 {
			continue
		}

		tagStats := make(map[string]any)
		var tagTotalLatency time.Duration
		var tagMinLatency time.Duration
		var tagMaxLatency time.Duration
		isFirstTag := true

		for _, latency := range latencies {
			if isFirstTag || latency < tagMinLatency {
				tagMinLatency = latency
			}

			if isFirstTag || latency > tagMaxLatency {
				tagMaxLatency = latency
			}

			tagTotalLatency += latency
			totalActiveInfoLatency += latency
			isFirstTag = false
			totalActiveInfoCount++
		}

		avgTagLatency := tagTotalLatency / time.Duration(len(latencies))
		avgTagLatencyMS := float64(avgTagLatency.Nanoseconds()) / 1e6

		tagStats["count"] = len(latencies)
		tagStats["sumMS"] = float64(tagTotalLatency.Nanoseconds()) / 1e6
		tagStats["averageMS"] = avgTagLatencyMS
		tagStats["minMS"] = float64(tagMinLatency.Nanoseconds()) / 1e6
		tagStats["maxMS"] = float64(tagMaxLatency.Nanoseconds()) / 1e6

		tagLatencyStats[tag] = tagStats
	}

	// 전체 getActiveInfoLatency 통계
	if totalActiveInfoCount > 0 {
		avgActiveInfoLatency := totalActiveInfoLatency / time.Duration(totalActiveInfoCount)
		avgActiveInfoLatencyMS := float64(avgActiveInfoLatency.Nanoseconds()) / 1e6

		activeInfoLatencyStats["totalSumMS"] = float64(totalActiveInfoLatency.Nanoseconds()) / 1e6
		activeInfoLatencyStats["averageMS"] = avgActiveInfoLatencyMS
		activeInfoLatencyStats["count"] = totalActiveInfoCount

		// avgTotalLatencyMS 대비 getActiveInfoLatency의 비율 계산 (completedUploadCount > 0인 경우만)
		if completedUploadCount > 0 && avgTotalLatencyMS > 0 {
			percentage := (avgActiveInfoLatencyMS / avgTotalLatencyMS) * 100
			activeInfoLatencyStats["percentageOfTotalLatency"] = percentage
		} else {
			activeInfoLatencyStats["percentageOfTotalLatency"] = float64(0)
		}
	} else {
		activeInfoLatencyStats["totalSumMS"] = float64(0)
		activeInfoLatencyStats["averageMS"] = float64(0)
		activeInfoLatencyStats["count"] = 0
		activeInfoLatencyStats["percentageOfTotalLatency"] = float64(0)
	}

	activeInfoLatencyStats["tagStats"] = tagLatencyStats

	// checkUploadIDExistsLatency 통계 계산
	checkUploadIDLatencyStats := make(map[string]any)
	checkUploadIDLatencyDetails := make(map[string]map[string]any)
	var totalCheckUploadIDLatency time.Duration
	totalCheckUploadIDCount := 0

	// 업로드ID별 지연 시간 합계 및 통계 계산
	for uploadID, latencies := range m.checkUploadIDExistsLatency {
		if len(latencies) == 0 {
			continue
		}

		// 해당 uploadID에 대한 업로드 정보 가져오기
		uploadMetric, exists := m.multipartUploadMetric[uploadID]
		if !exists || !uploadMetric.IsComplete {
			continue // 완료된 업로드만 포함
		}

		uploadStats := make(map[string]any)
		var uploadTotalLatency time.Duration
		var uploadMinLatency time.Duration
		var uploadMaxLatency time.Duration
		isFirstUpload := true

		for _, latency := range latencies {
			if isFirstUpload || latency < uploadMinLatency {
				uploadMinLatency = latency
			}

			if isFirstUpload || latency > uploadMaxLatency {
				uploadMaxLatency = latency
			}

			uploadTotalLatency += latency
			totalCheckUploadIDLatency += latency
			isFirstUpload = false
			totalCheckUploadIDCount++
		}

		// 업로드별 통계 계산
		avgUploadLatency := uploadTotalLatency / time.Duration(len(latencies))
		avgUploadLatencyMS := float64(avgUploadLatency.Nanoseconds()) / 1e6

		// 업로드별 지연 시간 통계
		uploadStats["count"] = len(latencies)
		uploadStats["sumMS"] = float64(uploadTotalLatency.Nanoseconds()) / 1e6
		uploadStats["averageMS"] = avgUploadLatencyMS
		uploadStats["minMS"] = float64(uploadMinLatency.Nanoseconds()) / 1e6
		uploadStats["maxMS"] = float64(uploadMaxLatency.Nanoseconds()) / 1e6

		// 업로드별 전체 업로드 대비 비율 (해당 업로드의 TotalLatency 대비)
		if uploadMetric.TotalLatency > 0 {
			uploadTotalLatencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
			// 체크 지연시간 총합을 밀리초로 변환
			uploadCheckTotalLatencyMS := float64(uploadTotalLatency.Nanoseconds()) / 1e6
			// 전체 업로드 지연시간 대비 체크 지연시간의 비율 계산
			percentage := (uploadCheckTotalLatencyMS * 100) / uploadTotalLatencyMS
			uploadStats["percentageOfCheckUploadIDLatency"] = percentage
		} else {
			uploadStats["percentageOfCheckUploadIDLatency"] = float64(0)
		}

		// 파트별 통계 계산 (해당 업로드의 각 파트 대비)
		if len(uploadMetric.Parts) > 0 {
			partPercentages := make(map[int]float64)

			for partID, part := range uploadMetric.Parts {
				partLatencyMS := float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6
				if partLatencyMS > 0 {
					// 파트별 지연시간 대비 체크 지연시간의 비율 계산
					partPercentage := (avgUploadLatencyMS * 100) / partLatencyMS
					partPercentages[partID] = partPercentage
				} else {
					partPercentages[partID] = 0
				}
			}

			uploadStats["partPercentages"] = partPercentages
		}

		checkUploadIDLatencyDetails[uploadID] = uploadStats
	}

	// 전체 checkUploadIDExistsLatency 통계
	if totalCheckUploadIDCount > 0 {
		avgCheckUploadIDLatency := totalCheckUploadIDLatency / time.Duration(totalCheckUploadIDCount)
		avgCheckUploadIDLatencyMS := float64(avgCheckUploadIDLatency.Nanoseconds()) / 1e6

		checkUploadIDLatencyStats["totalSumMS"] = float64(totalCheckUploadIDLatency.Nanoseconds()) / 1e6
		checkUploadIDLatencyStats["averageMS"] = avgCheckUploadIDLatencyMS
		checkUploadIDLatencyStats["count"] = totalCheckUploadIDCount

		// 전체 업로드 대비 비율 (TotalLatency 대비)
		if completedUploadCount > 0 && avgTotalLatencyMS > 0 {
			percentage := (avgCheckUploadIDLatencyMS * 100) / avgTotalLatencyMS
			checkUploadIDLatencyStats["percentageOfTotalLatency"] = percentage
		} else {
			checkUploadIDLatencyStats["percentageOfTotalLatency"] = float64(0)
		}

		// 파트 업로드 대비 비율 (PartLatency 대비)
		if totalPartCount > 0 && avgPartLatencyMS > 0 {
			percentage := (avgCheckUploadIDLatencyMS * 100) / avgPartLatencyMS
			checkUploadIDLatencyStats["percentageOfPartLatency"] = percentage
		} else {
			checkUploadIDLatencyStats["percentageOfPartLatency"] = float64(0)
		}
	} else {
		checkUploadIDLatencyStats["totalSumMS"] = float64(0)
		checkUploadIDLatencyStats["averageMS"] = float64(0)
		checkUploadIDLatencyStats["count"] = 0
		checkUploadIDLatencyStats["percentageOfTotalLatency"] = float64(0)
		checkUploadIDLatencyStats["percentageOfPartLatency"] = float64(0)
	}

	checkUploadIDLatencyStats["checkUploadIDStats"] = checkUploadIDLatencyDetails

	// readAllFileInfoLatency 통계 계산
	readAllFileInfoLatencyStats := make(map[string]any)
	readAllFileInfoLatencyDetails := make(map[string]map[string]any)
	var totalReadAllFileInfoLatency time.Duration
	totalReadAllFileInfoCount := 0

	// bucket/object별 지연 시간 합계 및 통계 계산
	for key, latencies := range m.readAllFileInfoLatency {
		if len(latencies) == 0 {
			continue
		}

		readAllFileInfoStats := make(map[string]any)
		var readAllFileInfoTotalLatency time.Duration
		var readAllFileInfoMinLatency time.Duration
		var readAllFileInfoMaxLatency time.Duration
		isFirstReadAllFileInfo := true

		for _, latency := range latencies {
			if isFirstReadAllFileInfo || latency < readAllFileInfoMinLatency {
				readAllFileInfoMinLatency = latency
			}

			if isFirstReadAllFileInfo || latency > readAllFileInfoMaxLatency {
				readAllFileInfoMaxLatency = latency
			}

			readAllFileInfoTotalLatency += latency
			totalReadAllFileInfoLatency += latency
			isFirstReadAllFileInfo = false
			totalReadAllFileInfoCount++
		}

		avgReadAllFileInfoLatency := readAllFileInfoTotalLatency / time.Duration(len(latencies))
		avgReadAllFileInfoLatencyMS := float64(avgReadAllFileInfoLatency.Nanoseconds()) / 1e6

		readAllFileInfoStats["count"] = len(latencies)
		readAllFileInfoStats["sumMS"] = float64(readAllFileInfoTotalLatency.Nanoseconds()) / 1e6
		readAllFileInfoStats["averageMS"] = avgReadAllFileInfoLatencyMS
		readAllFileInfoStats["minMS"] = float64(readAllFileInfoMinLatency.Nanoseconds()) / 1e6
		readAllFileInfoStats["maxMS"] = float64(readAllFileInfoMaxLatency.Nanoseconds()) / 1e6

		readAllFileInfoLatencyDetails[key] = readAllFileInfoStats
	}

	// 전체 readAllFileInfoLatency 통계
	if totalReadAllFileInfoCount > 0 {
		avgReadAllFileInfoLatency := totalReadAllFileInfoLatency / time.Duration(totalReadAllFileInfoCount)
		avgReadAllFileInfoLatencyMS := float64(avgReadAllFileInfoLatency.Nanoseconds()) / 1e6

		readAllFileInfoLatencyStats["totalSumMS"] = float64(totalReadAllFileInfoLatency.Nanoseconds()) / 1e6
		readAllFileInfoLatencyStats["averageMS"] = avgReadAllFileInfoLatencyMS
		readAllFileInfoLatencyStats["count"] = totalReadAllFileInfoCount

		// checkUploadIDExistsLatency와의 비율 계산
		// - checkUploadIDExistsLatency 평균 대비 readAllFileInfoLatency 평균의 비율
		avgCheckUploadIDLatencyMS := float64(0)
		if totalCheckUploadIDCount > 0 {
			avgCheckUploadIDLatency := totalCheckUploadIDLatency / time.Duration(totalCheckUploadIDCount)
			avgCheckUploadIDLatencyMS = float64(avgCheckUploadIDLatency.Nanoseconds()) / 1e6
		}

		if avgCheckUploadIDLatencyMS > 0 {
			percentage := (avgReadAllFileInfoLatencyMS * 100) / avgCheckUploadIDLatencyMS
			readAllFileInfoLatencyStats["percentageOfCheckUploadIDLatency"] = percentage
		} else {
			readAllFileInfoLatencyStats["percentageOfCheckUploadIDLatency"] = float64(0)
		}

		// 전체 업로드 대비 비율 (TotalLatency 대비)
		if completedUploadCount > 0 && avgTotalLatencyMS > 0 {
			percentage := (avgReadAllFileInfoLatencyMS * 100) / avgTotalLatencyMS
			readAllFileInfoLatencyStats["percentageOfTotalLatency"] = percentage
		} else {
			readAllFileInfoLatencyStats["percentageOfTotalLatency"] = float64(0)
		}

		// 파트 업로드 대비 비율 (PartLatency 대비)
		if totalPartCount > 0 && avgPartLatencyMS > 0 {
			percentage := (avgReadAllFileInfoLatencyMS * 100) / avgPartLatencyMS
			readAllFileInfoLatencyStats["percentageOfPartLatency"] = percentage
		} else {
			readAllFileInfoLatencyStats["percentageOfPartLatency"] = float64(0)
		}
	} else {
		readAllFileInfoLatencyStats["totalSumMS"] = float64(0)
		readAllFileInfoLatencyStats["averageMS"] = float64(0)
		readAllFileInfoLatencyStats["count"] = 0
		readAllFileInfoLatencyStats["percentageOfCheckUploadIDLatency"] = float64(0)
		readAllFileInfoLatencyStats["percentageOfTotalLatency"] = float64(0)
		readAllFileInfoLatencyStats["percentageOfPartLatency"] = float64(0)
	}

	readAllFileInfoLatencyStats["readAllFileInfoStats"] = readAllFileInfoLatencyDetails

	// 결과 맵 구성
	stats["totalLatency"] = totalLatencyStats
	stats["partLatency"] = partLatencyStats
	stats["activeInfoLatency"] = activeInfoLatencyStats
	stats["checkUploadIDExistsLatency"] = checkUploadIDLatencyStats
	stats["readAllFileInfoLatency"] = readAllFileInfoLatencyStats

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
			delete(m.checkUploadIDExistsLatency, uploadID)
			deletedCount++
		}
	}

	for tag := range m.getActiveInfoLatency {
		delete(m.getActiveInfoLatency, tag)
	}

	for key := range m.readAllFileInfoLatency {
		delete(m.readAllFileInfoLatency, key)
	}

	klog.Infof("[YBS] Cleared %d completed upload metrics and all latency records, %d in-progress uploads remain",
		deletedCount, len(m.multipartUploadMetric))
}

// ClearAllMetrics clears all metrics regardless of their completion status.
func (m *UploadLatencyManager) ClearAllMetrics() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.multipartUploadMetric = make(map[string]*MultipartUploadMetric)
	m.getActiveInfoLatency = make(map[string][]time.Duration)
	m.checkUploadIDExistsLatency = make(map[string][]time.Duration)
	m.readAllFileInfoLatency = make(map[string][]time.Duration)

	klog.Infof("[YBS] Cleared all upload metrics (both completed and in-progress) and all latency records")
}
