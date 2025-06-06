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
	"context"

	"k8s.io/klog/v2"
)

// startIDCFailureManager starts the IDC failure manager as part of the controller
func (c *Controller) startIDCFailureManager(ctx context.Context, stopCh <-chan struct{}) {
	klog.Info("[YBS] Initializing IDC Failure Manager...")

	// Create IDC Failure Manager
	idcManager := NewIDCFailureManager(
		c.dynamicClient,
		c.minioClientSet,
		c.namespacesToWatch,
	)

	// Start the manager
	go func() {
		if err := idcManager.Start(ctx); err != nil {
			klog.Errorf("[YBS] Failed to start IDC Failure Manager: %v", err)
			return
		}

		// Wait for stop signal
		<-stopCh
		idcManager.Stop()
		klog.Info("[YBS] IDC Failure Manager stopped")
	}()
}
