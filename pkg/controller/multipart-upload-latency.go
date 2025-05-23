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
	checkUploadIDExistsData    map[string][]CheckUploadIDExistsData
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

// LatencyStats is the statistics of latency data.
type LatencyStats struct {
	TotalSumMS  float64
	AverageMS   float64
	MinMS       float64
	MaxMS       float64
	StdDevMS    float64
	Ci95LowerMS float64
	Ci95UpperMS float64
	Count       int
	Percentages map[string]float64
}

// LatencyData is the data of latency.
type LatencyData struct {
	TotalLatency time.Duration
	MinLatency   time.Duration
	MaxLatency   time.Duration
	Count        int
	SumSquaredMS float64
	IsFirst      bool
}

// CheckUploadIDExistsData is the data of check upload id exists.
type CheckUploadIDExistsData struct {
	UploadID     string
	Bucket       string
	Object       string
	StartTime    time.Time
	CompleteTime time.Time
	Latency      time.Duration
}

// TimeInterval represents a time interval for overlap calculation
type TimeInterval struct {
	Start time.Time
	End   time.Time
}

// NewUploadLatencyManager creates a new UploadLatencyManager.
func NewUploadLatencyManager() *UploadLatencyManager {
	return &UploadLatencyManager{
		multipartUploadMetric:      make(map[string]*MultipartUploadMetric),
		getActiveInfoLatency:       make(map[string][]time.Duration),
		checkUploadIDExistsLatency: make(map[string][]time.Duration),
		readAllFileInfoLatency:     make(map[string][]time.Duration),
		checkUploadIDExistsData:    make(map[string][]CheckUploadIDExistsData),
	}
}

// RecordCheckUploadIDExistsLatency records the latency of checking upload id exists.
func (m *UploadLatencyManager) RecordCheckUploadIDExistsLatency(uploadID, bucket, object string, startTime time.Time, completeTime time.Time, latency time.Duration) {
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

	if completeTime.Before(startTime) {
		klog.Warningf("[YBS] Invalid time range: completeTime (%v) is before startTime (%v) for uploadID %s, ignoring",
			completeTime, startTime, uploadID)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkUploadIDExistsLatency[uploadID] = append(m.checkUploadIDExistsLatency[uploadID], latency)

	data := CheckUploadIDExistsData{
		UploadID:     uploadID,
		Bucket:       bucket,
		Object:       object,
		StartTime:    startTime,
		CompleteTime: completeTime,
		Latency:      latency,
	}
	m.checkUploadIDExistsData[uploadID] = append(m.checkUploadIDExistsData[uploadID], data)

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

	// 결과 맵 초기화
	stats := make(map[string]any)

	// 1. 기본 데이터 수집 단계
	uploadMetricData, totalData, partData, partDetails := m.collectBasicStats()

	// 2. 카테고리별 통계 계산
	// 모든 완료된 멀티파트 업로드의 총 업로드 시간에 대한 통계 데이터
	totalStats := m.calculateTotalLatencyStats(totalData)
	// 모든 완료된 멀티파트 업로드의 모든 파트에 대한 통계 데이터다.
	partStats := m.calculatePartLatencyStats(partData)
	activeStats := m.calculateActiveInfoStats(totalData)
	checkUploadIDStats := m.calculateCheckUploadIDStats(uploadMetricData, totalData, partData)
	readAllFileInfoStats := m.calculateReadAllFileInfoStats(checkUploadIDStats, totalData, partData)

	checkUploadIDStatsWithIntervals := m.calculateCheckUploadIDStatsWithIntervals(uploadMetricData, totalData, partData)

	// 3. 결과 맵 구성
	stats["totalLatency"] = totalStats
	stats["partLatency"] = partStats
	stats["activeInfoLatency"] = activeStats
	stats["checkUploadIDExistsLatency"] = checkUploadIDStats
	stats["checkUploadIDExistsLatencyWithIntervals"] = checkUploadIDStatsWithIntervals
	stats["readAllFileInfoLatency"] = readAllFileInfoStats

	// 4. 추가 정보
	stats["multipartUploadMetricCount"] = len(m.multipartUploadMetric)
	stats["completedUploadCount"] = totalData.Count
	stats["inProgressUploads"] = len(m.multipartUploadMetric) - totalData.Count
	stats["partDetails"] = partDetails

	return stats, nil
}

func (m *UploadLatencyManager) collectBasicStats() (map[string]*MultipartUploadMetric, LatencyData, LatencyData, map[string][]map[string]any) {
	uploadMetricData := make(map[string]*MultipartUploadMetric)
	totalData := LatencyData{IsFirst: true}
	partData := LatencyData{IsFirst: true}

	// key: uploadID
	// value: map slice 는 map<partID, ?> 와 map<latencyMs, ?> 같은 정보들이 담겨있다.
	partDetails := make(map[string][]map[string]any)

	// 모든 업로드를 순회하며 기본 통계 수집
	for uploadID, uploadMetric := range m.multipartUploadMetric {
		if !uploadMetric.IsComplete {
			continue
		}

		uploadMetricData[uploadID] = uploadMetric

		// 업로드 지연 시간 통계 수집
		if totalData.IsFirst || uploadMetric.TotalLatency < totalData.MinLatency {
			totalData.MinLatency = uploadMetric.TotalLatency
		}
		if totalData.IsFirst || uploadMetric.TotalLatency > totalData.MaxLatency {
			totalData.MaxLatency = uploadMetric.TotalLatency
		}

		totalData.TotalLatency += uploadMetric.TotalLatency
		latencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
		totalData.SumSquaredMS += latencyMS * latencyMS // 총 지연 시간의 제곱 합(표준편차  구하기 위함)
		totalData.Count++
		totalData.IsFirst = false

		// 파트 통계 수집 및 정렬
		parts := make([]map[string]any, 0, len(uploadMetric.Parts))
		partIDs := make([]int, 0, len(uploadMetric.Parts))
		for partID := range uploadMetric.Parts {
			partIDs = append(partIDs, partID)
		}
		sort.Ints(partIDs)

		// 파트별 통계 수집
		for _, partID := range partIDs {
			part := uploadMetric.Parts[partID]

			if partData.IsFirst || part.EachPartUploadLatency < partData.MinLatency {
				partData.MinLatency = part.EachPartUploadLatency
			}
			if partData.IsFirst || part.EachPartUploadLatency > partData.MaxLatency {
				partData.MaxLatency = part.EachPartUploadLatency
			}

			partData.TotalLatency += part.EachPartUploadLatency
			partLatencyMS := float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6
			partData.SumSquaredMS += partLatencyMS * partLatencyMS // 총 지연 시간의 제곱 합(표준편차  구하기 위함)
			partData.Count++
			partData.IsFirst = false

			// 파트 상세 정보 저장
			parts = append(parts, map[string]any{
				"partID":    part.PartID,
				"latencyMS": float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6,
			})
		}

		partDetails[uploadID] = parts
	}

	return uploadMetricData, totalData, partData, partDetails
}

// 지연 시간 통계 계산 유틸리티 함수
func calculateLatencyStats(data LatencyData) map[string]any {
	stats := make(map[string]any)

	if data.Count <= 0 {
		stats["averageMS"] = float64(0)
		stats["minMS"] = float64(0)
		stats["maxMS"] = float64(0)
		stats["stdDevMS"] = float64(0)
		stats["ci95LowerMS"] = float64(0)
		stats["ci95UpperMS"] = float64(0)
		stats["count"] = 0
		stats["totalSumMS"] = float64(0)
		return stats
	}

	avgLatency := data.TotalLatency / time.Duration(data.Count)
	avgLatencyMS := float64(avgLatency.Nanoseconds()) / 1e6

	// 분산 계산
	varianceMS := (data.SumSquaredMS / float64(data.Count)) - (avgLatencyMS * avgLatencyMS)
	if varianceMS < 0 {
		varianceMS = 0 // 수치적 오류로 인한 음수 방지
	}

	// 표준편차 계산
	stdDevMS := math.Sqrt(varianceMS)

	// 95% 신뢰구간 계산
	standardErrorMS := stdDevMS / math.Sqrt(float64(data.Count))
	ci95LowerMS := avgLatencyMS - 1.96*standardErrorMS
	ci95UpperMS := avgLatencyMS + 1.96*standardErrorMS

	// 밀리초 단위로 통계 저장
	stats["totalSumMS"] = float64(data.TotalLatency.Nanoseconds()) / 1e6
	stats["averageMS"] = avgLatencyMS
	stats["minMS"] = float64(data.MinLatency.Nanoseconds()) / 1e6
	stats["maxMS"] = float64(data.MaxLatency.Nanoseconds()) / 1e6
	stats["stdDevMS"] = stdDevMS
	stats["ci95LowerMS"] = ci95LowerMS
	stats["ci95UpperMS"] = ci95UpperMS
	stats["count"] = data.Count

	return stats
}

// 비율 계산 유틸리티 함수
func calculatePercentage(part, total float64) float64 {
	if total <= 0 {
		return 0
	}
	return (part * 100) / total
}

// TotalLatency 통계 계산
func (m *UploadLatencyManager) calculateTotalLatencyStats(data LatencyData) map[string]any {
	return calculateLatencyStats(data)
}

// PartLatency 통계 계산
func (m *UploadLatencyManager) calculatePartLatencyStats(data LatencyData) map[string]any {
	return calculateLatencyStats(data)
}

// ActiveInfoLatency 통계 계산
func (m *UploadLatencyManager) calculateActiveInfoStats(totalData LatencyData) map[string]any {
	activeInfoData := LatencyData{IsFirst: true}
	tagStats := make(map[string]map[string]any)

	// 각 태그별 지연 시간 수집
	for tag, latencies := range m.getActiveInfoLatency {
		if len(latencies) == 0 {
			continue
		}

		tagData := LatencyData{IsFirst: true}

		for _, latency := range latencies {
			if tagData.IsFirst || latency < tagData.MinLatency {
				tagData.MinLatency = latency
			}
			if tagData.IsFirst || latency > tagData.MaxLatency {
				tagData.MaxLatency = latency
			}

			tagData.TotalLatency += latency
			tagLatencyMS := float64(latency.Nanoseconds()) / 1e6
			tagData.SumSquaredMS += tagLatencyMS * tagLatencyMS
			tagData.Count++
			tagData.IsFirst = false

			// 전체 activeInfo 통계에도 추가
			activeInfoData.TotalLatency += latency
			activeInfoData.Count++

			if activeInfoData.IsFirst || latency < activeInfoData.MinLatency {
				activeInfoData.MinLatency = latency
			}
			if activeInfoData.IsFirst || latency > activeInfoData.MaxLatency {
				activeInfoData.MaxLatency = latency
			}
			activeInfoData.IsFirst = false
		}

		tagStats[tag] = calculateLatencyStats(tagData)
	}

	// 전체 activeInfo 통계 계산
	activeInfoStats := calculateLatencyStats(activeInfoData)

	// 총 업로드 시간 대비 비율 계산
	if activeInfoData.Count > 0 && totalData.Count > 0 {
		totalLatencyMS := float64(totalData.TotalLatency.Nanoseconds()) / 1e6
		activeInfoLatencyMS := float64(activeInfoData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(activeInfoLatencyMS, totalLatencyMS)
		activeInfoStats["percentageOfTotalLatency"] = percentage
	} else {
		activeInfoStats["percentageOfTotalLatency"] = float64(0)
	}

	activeInfoStats["tagStats"] = tagStats
	return activeInfoStats
}

// CheckUploadIDExistsLatency 통계 계산
func (m *UploadLatencyManager) calculateCheckUploadIDStats(uploadMetricData map[string]*MultipartUploadMetric, totalData LatencyData, partData LatencyData) map[string]any {
	checkUploadIDData := LatencyData{IsFirst: true}
	checkUploadIDDetails := make(map[string]map[string]any)

	// 업로드ID별 지연 시간 수집
	// 1GB * 50번 테스트 라면 (64+1) * 50번 호출이 발생한다. 64는 파트수 1번은 CompleteMultipartUpload 호출이다.
	for uploadID, latencies := range m.checkUploadIDExistsLatency {
		if len(latencies) == 0 {
			continue
		}

		// 완료된 업로드만 포함
		uploadMetric, exists := uploadMetricData[uploadID]
		if !exists || !uploadMetric.IsComplete {
			continue
		}

		checkUploadIDExistData := LatencyData{IsFirst: true}

		for _, latency := range latencies {
			if checkUploadIDExistData.IsFirst || latency < checkUploadIDExistData.MinLatency {
				checkUploadIDExistData.MinLatency = latency
			}
			if checkUploadIDExistData.IsFirst || latency > checkUploadIDExistData.MaxLatency {
				checkUploadIDExistData.MaxLatency = latency
			}

			checkUploadIDExistData.TotalLatency += latency
			latencyMS := float64(latency.Nanoseconds()) / 1e6
			checkUploadIDExistData.SumSquaredMS += latencyMS * latencyMS
			checkUploadIDExistData.Count++
			checkUploadIDExistData.IsFirst = false

			// 전체 checkUploadID 통계에도 추가
			checkUploadIDData.TotalLatency += latency
			checkUploadIDData.Count++

			if checkUploadIDData.IsFirst || latency < checkUploadIDData.MinLatency {
				checkUploadIDData.MinLatency = latency
			}
			if checkUploadIDData.IsFirst || latency > checkUploadIDData.MaxLatency {
				checkUploadIDData.MaxLatency = latency
			}
			checkUploadIDData.IsFirst = false
		}

		// 하나의 uploadId 에 대한 CheckUploadIDExists 호출들의 통계 계산
		checkUploadIDExisStats := calculateLatencyStats(checkUploadIDExistData)

		// 전체 업로드 대비 CheckUploadIDExists 비율 계산
		if uploadMetric.TotalLatency > 0 {
			uploadTotalLatencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
			uploadCheckTotalLatencyMS := float64(checkUploadIDExistData.TotalLatency.Nanoseconds()) / 1e6
			percentage := calculatePercentage(uploadCheckTotalLatencyMS, uploadTotalLatencyMS)
			checkUploadIDExisStats["percentageOfUploadLatency"] = percentage
		} else {
			checkUploadIDExisStats["percentageOfUploadLatency"] = float64(0)
		}

		// 파트별 백분율 계산
		if len(uploadMetric.Parts) > 0 {
			partPercentages := make(map[int]float64)
			avgUploadLatencyMS := checkUploadIDExisStats["averageMS"].(float64)

			for partID, part := range uploadMetric.Parts {
				partLatencyMS := float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6
				if partLatencyMS > 0 {
					partPercentage := calculatePercentage(avgUploadLatencyMS, partLatencyMS)
					partPercentages[partID] = partPercentage
				} else {
					partPercentages[partID] = 0
				}
			}

			checkUploadIDExisStats["partPercentages"] = partPercentages
		}

		checkUploadIDDetails[uploadID] = checkUploadIDExisStats
	}

	// 모든 업로드에 걸친 CheckUploadIDExists 통계 계산(반복횟수 포함된것)
	checkUploadIDStats := calculateLatencyStats(checkUploadIDData)

	// 전체 업로드 대비 비율 계산 (TotalLatency 대비) - 총합 기준
	// 모든 CheckUploadIDExists 호출의 총 소요 시간과 모든 업로드의 총 소요 시간을 비교
	if checkUploadIDData.Count > 0 && totalData.Count > 0 {
		// 모든 업로드에서 발생한 모든 CheckUploadIDExists 호출의 누적 소요 시간
		totalCheckLatencyMS := float64(checkUploadIDData.TotalLatency.Nanoseconds()) / 1e6
		// 모든 업로드의 시작부터 완료까지 소요된 총 시간의 합
		totalUploadLatencyMS := float64(totalData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalCheckLatencyMS, totalUploadLatencyMS)
		checkUploadIDStats["percentageOfTotalLatency"] = percentage
	} else {
		checkUploadIDStats["percentageOfTotalLatency"] = float64(0)
	}

	// 파트 업로드 대비 비율 계산 (PartLatency 대비) - 총합 기준
	if checkUploadIDData.Count > 0 && partData.Count > 0 {
		totalCheckLatencyMS := float64(checkUploadIDData.TotalLatency.Nanoseconds()) / 1e6
		totalPartLatencyMS := float64(partData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalCheckLatencyMS, totalPartLatencyMS)
		checkUploadIDStats["percentageOfPartLatency"] = percentage
	} else {
		checkUploadIDStats["percentageOfPartLatency"] = float64(0)
	}

	checkUploadIDStats["checkUploadIDStats"] = checkUploadIDDetails
	return checkUploadIDStats
}

// ReadAllFileInfoLatency 통계 계산
func (m *UploadLatencyManager) calculateReadAllFileInfoStats(checkUploadIDStats map[string]any, totalData LatencyData, partData LatencyData) map[string]any {
	readAllFileInfoData := LatencyData{IsFirst: true}
	readAllFileInfoDetails := make(map[string]map[string]any)

	// bucket/object별 지연 시간 수집
	for key, latencies := range m.readAllFileInfoLatency {
		if len(latencies) == 0 {
			continue
		}

		keyData := LatencyData{IsFirst: true}

		for _, latency := range latencies {
			if keyData.IsFirst || latency < keyData.MinLatency {
				keyData.MinLatency = latency
			}
			if keyData.IsFirst || latency > keyData.MaxLatency {
				keyData.MaxLatency = latency
			}

			keyData.TotalLatency += latency
			latencyMS := float64(latency.Nanoseconds()) / 1e6
			keyData.SumSquaredMS += latencyMS * latencyMS
			keyData.Count++
			keyData.IsFirst = false

			// 전체 readAllFileInfo 통계에도 추가
			readAllFileInfoData.TotalLatency += latency
			readAllFileInfoData.Count++

			if readAllFileInfoData.IsFirst || latency < readAllFileInfoData.MinLatency {
				readAllFileInfoData.MinLatency = latency
			}
			if readAllFileInfoData.IsFirst || latency > readAllFileInfoData.MaxLatency {
				readAllFileInfoData.MaxLatency = latency
			}
			readAllFileInfoData.IsFirst = false
		}

		readAllFileInfoDetails[key] = calculateLatencyStats(keyData)
	}

	// 전체 readAllFileInfo 통계 계산
	readAllFileInfoStats := calculateLatencyStats(readAllFileInfoData)

	// checkUploadIDExistsLatency와의 비율 계산 - 총합 기준
	if readAllFileInfoData.Count > 0 && checkUploadIDStats["count"].(int) > 0 {
		totalReadLatencyMS := float64(readAllFileInfoData.TotalLatency.Nanoseconds()) / 1e6
		totalCheckLatencyMS := checkUploadIDStats["totalSumMS"].(float64)
		if totalCheckLatencyMS > 0 {
			percentage := calculatePercentage(totalReadLatencyMS, totalCheckLatencyMS)
			readAllFileInfoStats["percentageOfCheckUploadIDLatency"] = percentage
		} else {
			readAllFileInfoStats["percentageOfCheckUploadIDLatency"] = float64(0)
		}
	} else {
		readAllFileInfoStats["percentageOfCheckUploadIDLatency"] = float64(0)
	}

	// 전체 업로드 대비 비율 계산 (TotalLatency 대비) - 총합 기준
	if readAllFileInfoData.Count > 0 && totalData.Count > 0 {
		totalReadLatencyMS := float64(readAllFileInfoData.TotalLatency.Nanoseconds()) / 1e6
		totalUploadLatencyMS := float64(totalData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalReadLatencyMS, totalUploadLatencyMS)
		readAllFileInfoStats["percentageOfTotalLatency"] = percentage
	} else {
		readAllFileInfoStats["percentageOfTotalLatency"] = float64(0)
	}

	// 파트 업로드 대비 비율 계산 (PartLatency 대비) - 총합 기준
	if readAllFileInfoData.Count > 0 && partData.Count > 0 {
		totalReadLatencyMS := float64(readAllFileInfoData.TotalLatency.Nanoseconds()) / 1e6
		totalPartLatencyMS := float64(partData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalReadLatencyMS, totalPartLatencyMS)
		readAllFileInfoStats["percentageOfPartLatency"] = percentage
	} else {
		readAllFileInfoStats["percentageOfPartLatency"] = float64(0)
	}

	readAllFileInfoStats["readAllFileInfoStats"] = readAllFileInfoDetails
	return readAllFileInfoStats
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

func mergeTimeIntervals(intervals []TimeInterval) time.Duration {
	if len(intervals) == 0 {
		return 0
	}

	// 시작 시간 기준으로 오름차순 정렬
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i].Start.Before(intervals[j].Start)
	})

	var merged []TimeInterval // 최종적으로 병합된 (겹치지 않는) 간격들을 저장
	current := intervals[0]   // 정렬된 첫 번째 간격을 '현재 병합 중인 간격'으로 설정

	for i := 1; i < len(intervals); i++ {
		next := intervals[i]

		// 현재 간격(current)과 다음 간격(next)이 겹치는지 확인
		// - 현재 간격의 끝이 다음 간격의 시작과 같거나 그 이후라면 겹치거나 바로 이어붙는 경우
		if !current.End.Before(next.Start) {
			// 겹치는 경우: 현재 간격을 확장하여 병합
			// 다음 간격의 끝(next.End)이 현재 간격의 끝(current.End)보다 뒤라면,
			// 현재 간격의 끝을 다음 간격의 끝으로 확장
			if next.End.After(current.End) {
				current.End = next.End
			}
			// 만약 next 간격이 current 간격에 완전히 포함되는 경우 (next.End <= current.End),
			// current.End는 변경되지 않으며, 이는 올바른 동작
		} else {
			// 겹치지 않는 경우:
			// 현재까지 만들어진 병합 간격(current)은 완료된 것으로 보고 merged 슬라이스에 추가
			merged = append(merged, current)
			current = next
		}
	}

	// 마지막 간격(current)을 병합 간격 슬라이스에 추가
	merged = append(merged, current)

	// 병합된 모든 간격들의 총 유효 시간 계산
	var totalDuration time.Duration
	for _, interval := range merged {
		totalDuration += interval.End.Sub(interval.Start)
	}

	return totalDuration
}

// calculateCheckUploadIDStatsWithIntervals calculates CheckUploadIDExists statistics using time interval merging
func (m *UploadLatencyManager) calculateCheckUploadIDStatsWithIntervals(uploadMetricData map[string]*MultipartUploadMetric, totalData LatencyData, partData LatencyData) map[string]any {
	checkUploadIDData := LatencyData{IsFirst: true}
	checkUploadIDDetails := make(map[string]map[string]any)

	// 전체 업로드에 걸친 시간 간격 수집
	var allIntervals []TimeInterval
	totalEffectiveDuration := time.Duration(0)

	// 업로드ID별 지연 시간 수집 및 시간 간격 분석
	for uploadID, dataList := range m.checkUploadIDExistsData {
		if len(dataList) == 0 {
			continue
		}

		// 완료된 업로드만 포함
		uploadMetric, exists := uploadMetricData[uploadID]
		if !exists || !uploadMetric.IsComplete {
			continue
		}

		checkUploadIDExistData := LatencyData{IsFirst: true}
		var checkUploadIDIntervals []TimeInterval

		for _, data := range dataList {
			// 기존 방식의 통계 수집
			if checkUploadIDExistData.IsFirst || data.Latency < checkUploadIDExistData.MinLatency {
				checkUploadIDExistData.MinLatency = data.Latency
			}
			if checkUploadIDExistData.IsFirst || data.Latency > checkUploadIDExistData.MaxLatency {
				checkUploadIDExistData.MaxLatency = data.Latency
			}

			checkUploadIDExistData.TotalLatency += data.Latency
			latencyMS := float64(data.Latency.Nanoseconds()) / 1e6
			checkUploadIDExistData.SumSquaredMS += latencyMS * latencyMS
			checkUploadIDExistData.Count++
			checkUploadIDExistData.IsFirst = false

			// 전체 checkUploadID 통계에도 추가
			checkUploadIDData.TotalLatency += data.Latency
			checkUploadIDData.Count++

			if checkUploadIDData.IsFirst || data.Latency < checkUploadIDData.MinLatency {
				checkUploadIDData.MinLatency = data.Latency
			}
			if checkUploadIDData.IsFirst || data.Latency > checkUploadIDData.MaxLatency {
				checkUploadIDData.MaxLatency = data.Latency
			}
			checkUploadIDData.IsFirst = false

			// CheckUploadIDExists 호출의 시간 간격 수집
			checkInterval := TimeInterval{
				Start: data.StartTime,
				End:   data.CompleteTime,
			}
			checkUploadIDIntervals = append(checkUploadIDIntervals, checkInterval)
			allIntervals = append(allIntervals, checkInterval)
		}

		// 해당 업로드ID의 CheckUploadIDExists 호출들의 유효 시간 계산
		checkUploadIDEffectiveDuration := mergeTimeIntervals(checkUploadIDIntervals)

		// 하나의 uploadId에 대한 CheckUploadIDExists 호출들의 통계 계산
		checkUploadIDExisStats := calculateLatencyStats(checkUploadIDExistData)

		// 기존 방식의 비율 계산 (단순 합산)
		if uploadMetric.TotalLatency > 0 {
			uploadTotalLatencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
			uploadCheckTotalLatencyMS := float64(checkUploadIDExistData.TotalLatency.Nanoseconds()) / 1e6
			percentage := calculatePercentage(uploadCheckTotalLatencyMS, uploadTotalLatencyMS)
			checkUploadIDExisStats["percentageOfUploadLatency"] = percentage
		} else {
			checkUploadIDExisStats["percentageOfUploadLatency"] = float64(0)
		}

		// 개선된 방식의 비율 계산 (시간 간격 병합)
		if uploadMetric.TotalLatency > 0 {
			uploadTotalLatencyMS := float64(uploadMetric.TotalLatency.Nanoseconds()) / 1e6
			uploadEffectiveLatencyMS := float64(checkUploadIDEffectiveDuration.Nanoseconds()) / 1e6
			effectivePercentage := calculatePercentage(uploadEffectiveLatencyMS, uploadTotalLatencyMS)
			checkUploadIDExisStats["effectivePercentageOfUploadLatency"] = effectivePercentage
		} else {
			checkUploadIDExisStats["effectivePercentageOfUploadLatency"] = float64(0)
		}

		// 유효 시간 정보 추가
		checkUploadIDExisStats["effectiveDurationMS"] = float64(checkUploadIDEffectiveDuration.Nanoseconds()) / 1e6
		checkUploadIDExisStats["intervalCount"] = len(checkUploadIDIntervals)

		// 파트별 백분율 계산 (기존 방식 유지)
		if len(uploadMetric.Parts) > 0 {
			partPercentages := make(map[int]float64)
			avgUploadLatencyMS := checkUploadIDExisStats["averageMS"].(float64)

			for partID, part := range uploadMetric.Parts {
				partLatencyMS := float64(part.EachPartUploadLatency.Nanoseconds()) / 1e6
				if partLatencyMS > 0 {
					partPercentage := calculatePercentage(avgUploadLatencyMS, partLatencyMS)
					partPercentages[partID] = partPercentage
				} else {
					partPercentages[partID] = 0
				}
			}

			checkUploadIDExisStats["partPercentages"] = partPercentages
		}

		checkUploadIDDetails[uploadID] = checkUploadIDExisStats
		totalEffectiveDuration += checkUploadIDEffectiveDuration
	}

	// 전체 유효 시간 계산 (모든 업로드의 시간 간격 병합)
	globalEffectiveDuration := mergeTimeIntervals(allIntervals)

	// 모든 업로드에 걸친 CheckUploadIDExists 통계 계산
	checkUploadIDStats := calculateLatencyStats(checkUploadIDData)

	// 기존 방식의 전체 업로드 대비 비율 계산 (단순 합산)
	if checkUploadIDData.Count > 0 && totalData.Count > 0 {
		totalCheckLatencyMS := float64(checkUploadIDData.TotalLatency.Nanoseconds()) / 1e6
		totalUploadLatencyMS := float64(totalData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalCheckLatencyMS, totalUploadLatencyMS)
		checkUploadIDStats["percentageOfTotalLatency"] = percentage
	} else {
		checkUploadIDStats["percentageOfTotalLatency"] = float64(0)
	}

	// 개선된 방식의 전체 업로드 대비 비율 계산 (시간 간격 병합)
	if totalData.Count > 0 {
		totalUploadLatencyMS := float64(totalData.TotalLatency.Nanoseconds()) / 1e6
		globalEffectiveLatencyMS := float64(globalEffectiveDuration.Nanoseconds()) / 1e6
		effectivePercentage := calculatePercentage(globalEffectiveLatencyMS, totalUploadLatencyMS)
		checkUploadIDStats["effectivePercentageOfTotalLatency"] = effectivePercentage
	} else {
		checkUploadIDStats["effectivePercentageOfTotalLatency"] = float64(0)
	}

	// 파트 업로드 대비 비율 계산 (기존 방식)
	if checkUploadIDData.Count > 0 && partData.Count > 0 {
		totalCheckLatencyMS := float64(checkUploadIDData.TotalLatency.Nanoseconds()) / 1e6
		totalPartLatencyMS := float64(partData.TotalLatency.Nanoseconds()) / 1e6
		percentage := calculatePercentage(totalCheckLatencyMS, totalPartLatencyMS)
		checkUploadIDStats["percentageOfPartLatency"] = percentage
	} else {
		checkUploadIDStats["percentageOfPartLatency"] = float64(0)
	}

	// 개선된 방식의 파트 업로드 대비 비율 계산
	if partData.Count > 0 {
		totalPartLatencyMS := float64(partData.TotalLatency.Nanoseconds()) / 1e6
		globalEffectiveLatencyMS := float64(globalEffectiveDuration.Nanoseconds()) / 1e6
		effectivePercentage := calculatePercentage(globalEffectiveLatencyMS, totalPartLatencyMS)
		checkUploadIDStats["effectivePercentageOfPartLatency"] = effectivePercentage
	} else {
		checkUploadIDStats["effectivePercentageOfPartLatency"] = float64(0)
	}

	// 추가 정보
	checkUploadIDStats["globalEffectiveDurationMS"] = float64(globalEffectiveDuration.Nanoseconds()) / 1e6
	checkUploadIDStats["totalEffectiveDurationMS"] = float64(totalEffectiveDuration.Nanoseconds()) / 1e6
	checkUploadIDStats["totalIntervalCount"] = len(allIntervals)
	checkUploadIDStats["checkUploadIDStats"] = checkUploadIDDetails

	return checkUploadIDStats
}
