package controller

import (
	"net/http"
	"strconv"

	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/util/random"
	"github.com/mhsanaei/3x-ui/v2/web/service"

	"github.com/gin-gonic/gin"
)

// SyncController handles configuration synchronization endpoints.
// Public endpoints (/sync/*) use X-Sync-Secret header auth instead of session auth.
// Admin endpoints (/panel/api/sync/*) use session auth via APIController middleware.
type SyncController struct {
	syncService    service.SyncService
	settingService service.SettingService
}

// NewSyncController creates a new SyncController instance.
func NewSyncController() *SyncController {
	return &SyncController{}
}

// InitAdminRouter registers sync node management routes under the authenticated API group.
func (s *SyncController) InitAdminRouter(g *gin.RouterGroup) {
	sync := g.Group("/sync")
	sync.GET("/nodes", s.getNodes)
	sync.POST("/nodes/add", s.addNode)
	sync.POST("/nodes/del/:id", s.delNode)
	sync.POST("/nodes/update/:id", s.updateNode)
	sync.POST("/nodes/test/:id", s.testNode)
	sync.GET("/secret", s.getSecret)
	sync.POST("/secret/regenerate", s.regenerateSecret)
	sync.POST("/fullsync", s.fullSync)
}

// checkSyncSecret validates the X-Sync-Secret header against the stored cluster secret.
func (s *SyncController) checkSyncSecret(c *gin.Context) bool {
	provided := c.GetHeader("X-Sync-Secret")
	if provided == "" {
		return false
	}
	stored, err := s.settingService.GetSyncSecret()
	if err != nil || stored == "" {
		return false
	}
	return provided == stored
}

// ReceivePush handles incoming sync payloads from the master node (slave endpoint).
// POST /sync/push  — authenticated by X-Sync-Secret header.
func (s *SyncController) ReceivePush(c *gin.Context) {
	enabled, err := s.settingService.GetSyncEnable()
	if err != nil || !enabled {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	if !s.checkSyncSecret(c) {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}

	var payload service.SyncPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.syncService.ApplySyncPayload(&payload); err != nil {
		logger.Warning("sync apply payload failed:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// ReceiveTraffic handles incremental traffic reports from slave nodes (master endpoint).
// POST /sync/traffic?secret=<slave_syncSecret>  — authenticated by the slave's own secret.
func (s *SyncController) ReceiveTraffic(c *gin.Context) {
	enabled, err := s.settingService.GetSyncEnable()
	if err != nil || !enabled {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	// Validate secret: must match one of the configured slave nodes
	provided := c.Query("secret")
	if !s.syncService.ValidateNodeSecret(provided) {
		c.AbortWithStatus(http.StatusForbidden)
		return
	}

	var delta service.TrafficDelta
	if err := c.ShouldBindJSON(&delta); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := s.syncService.HandleTrafficReport(&delta); err != nil {
		logger.Warning("traffic report handler failed:", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"success": true})
}

// --- Admin API handlers (session-authenticated via APIController middleware) ---

// getNodes returns all configured sync nodes.
func (s *SyncController) getNodes(c *gin.Context) {
	nodes, err := s.syncService.GetSyncNodes()
	if err != nil {
		jsonMsg(c, "get sync nodes", err)
		return
	}
	jsonObj(c, nodes, nil)
}

// addNode creates a new sync node entry.
func (s *SyncController) addNode(c *gin.Context) {
	var node model.SyncNode
	if err := c.ShouldBind(&node); err != nil {
		jsonMsg(c, "add sync node", err)
		return
	}
	if err := s.syncService.AddSyncNode(&node); err != nil {
		jsonMsg(c, "add sync node", err)
		return
	}
	jsonObj(c, node, nil)
}

// delNode removes a sync node by ID.
func (s *SyncController) delNode(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		jsonMsg(c, "del sync node", err)
		return
	}
	if err := s.syncService.DelSyncNode(id); err != nil {
		jsonMsg(c, "del sync node", err)
		return
	}
	jsonMsg(c, "ok", nil)
}

// updateNode updates an existing sync node.
func (s *SyncController) updateNode(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		jsonMsg(c, "update sync node", err)
		return
	}
	var node model.SyncNode
	if err := c.ShouldBind(&node); err != nil {
		jsonMsg(c, "update sync node", err)
		return
	}
	node.Id = id
	if err := s.syncService.UpdateSyncNode(&node); err != nil {
		jsonMsg(c, "update sync node", err)
		return
	}
	jsonObj(c, node, nil)
}

// testNode tests connectivity and authentication to a sync node.
func (s *SyncController) testNode(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil {
		jsonMsg(c, "test sync node", err)
		return
	}
	if err := s.syncService.TestNodeConnection(id); err != nil {
		jsonMsg(c, "test sync node", err)
		return
	}
	jsonMsg(c, "ok", nil)
}

// getSecret returns the current node's syncSecret for display in the admin UI.
func (s *SyncController) getSecret(c *gin.Context) {
	secret, err := s.settingService.GetSyncSecret()
	if err != nil {
		jsonMsg(c, "get sync secret", err)
		return
	}
	jsonObj(c, gin.H{"secret": secret}, nil)
}

// fullSync triggers an immediate full sync push to all enabled slave nodes.
func (s *SyncController) fullSync(c *gin.Context) {
	s.syncService.FullSync()
	jsonMsg(c, "full sync", nil)
}

// regenerateSecret generates a new random syncSecret and saves it.
func (s *SyncController) regenerateSecret(c *gin.Context) {
	newSecret := random.Seq(32)
	if err := s.settingService.SetSyncSecret(newSecret); err != nil {
		jsonMsg(c, "regenerate sync secret", err)
		return
	}
	jsonObj(c, gin.H{"secret": newSecret}, nil)
}
