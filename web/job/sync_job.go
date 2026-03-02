package job

import (
	"github.com/mhsanaei/3x-ui/v2/web/service"
)

// SyncJob performs a periodic full configuration sync from master to all enabled slave nodes.
// It acts as a compensating mechanism to ensure consistency even if real-time pushes failed.
type SyncJob struct {
	syncService service.SyncService
}

// NewSyncJob creates a new SyncJob instance.
func NewSyncJob() *SyncJob {
	return new(SyncJob)
}

// Run executes the full sync to all enabled slave nodes.
func (j *SyncJob) Run() {
	j.syncService.FullSync()
}
