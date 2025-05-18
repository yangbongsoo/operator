// Copyright (C) 2020, MinIO, Inc.
//
// This code is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License, version 3,
// as published by the Free Software Foundation.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License, version 3,
// along with this program.  If not, see <http://www.gnu.org/licenses/>

package controller

import (
	"net/http"
	"time"

	"github.com/minio/operator/pkg/common"

	"github.com/gorilla/mux"
)

func configureHTTPUpgradeServer(controller *Controller) *http.Server {
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	router.Methods(http.MethodGet).
		PathPrefix(common.WebhookAPIUpdate).
		Handler(http.StripPrefix(common.WebhookAPIUpdate, http.FileServer(http.Dir(updatePath))))

	router.HandleFunc("/report", func(w http.ResponseWriter, r *http.Request) {
		controller.reportHandler(w, r)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart-upload-latency-start", func(w http.ResponseWriter, r *http.Request) {
		controller.multipartUploadLatencyStartHandler(w, r)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart-upload-latency-part", func(w http.ResponseWriter, r *http.Request) {
		controller.multipartUploadLatencyPartHandler(w, r)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart-upload-latency-complete", func(w http.ResponseWriter, r *http.Request) {
		controller.multipartUploadLatencyCompleteHandler(w, r)
	}).Methods(http.MethodPost)

	router.HandleFunc("/multipart-upload-latency-stats", controller.getLatencyStatsHandler).Methods(http.MethodGet)

	router.HandleFunc("/multipart-upload-latency-clear-all", controller.clearAllMetricsHandler).Methods(http.MethodGet)

	router.HandleFunc("/get-active-info-latency", controller.getActiveInfoLatencyHandler).Methods(http.MethodPost)

	router.NotFoundHandler = http.NotFoundHandler()

	s := &http.Server{
		Addr:           ":" + common.UpgradeServerPort,
		Handler:        router,
		ReadTimeout:    time.Minute,
		WriteTimeout:   time.Minute,
		MaxHeaderBytes: 1 << 20,
	}

	return s
}
