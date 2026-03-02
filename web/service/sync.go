package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/mhsanaei/3x-ui/v2/database"
	"github.com/mhsanaei/3x-ui/v2/database/model"
	"github.com/mhsanaei/3x-ui/v2/logger"
	"github.com/mhsanaei/3x-ui/v2/xray"
	"gorm.io/gorm"
)

// SyncSettings holds non-system settings that are synchronized from master to slaves.
type SyncSettings struct {
	XrayTemplateConfig string `json:"xrayTemplateConfig"`
	SubPath            string `json:"subPath"`
	SubTitle           string `json:"subTitle"`
	SubEnableRouting   bool   `json:"subEnableRouting"`
	SubRoutingRules    string `json:"subRoutingRules"`
	SubJsonFragment    string `json:"subJsonFragment"`
	SubJsonNoises      string `json:"subJsonNoises"`
	SubJsonMux         string `json:"subJsonMux"`
	SubJsonRules       string `json:"subJsonRules"`
}

// SyncPayload is the data pushed from master to slave nodes.
type SyncPayload struct {
	Timestamp int64                `json:"timestamp"`
	Inbounds  []model.Inbound      `json:"inbounds"`
	Traffics  []xray.ClientTraffic `json:"traffics"`
	Settings  *SyncSettings        `json:"settings,omitempty"`
	MasterURL string               `json:"masterURL,omitempty"` // master's public base URL; slaves use this to auto-configure traffic reporting URI
}

// TrafficDelta is the incremental traffic payload sent from slave to master,
// using the same format as the existing externalTrafficInformURI mechanism.
type TrafficDelta struct {
	ClientTraffics  []*xray.ClientTraffic `json:"clientTraffics"`
	InboundTraffics []*xray.Traffic       `json:"inboundTraffics"`
}

// SyncService handles configuration synchronization between master and slave nodes.
type SyncService struct {
	inboundService InboundService
	settingService SettingService
	xrayService    XrayService
}

var syncHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

// GetSyncNodes returns all sync nodes from the database.
func (s *SyncService) GetSyncNodes() ([]*model.SyncNode, error) {
	db := database.GetDB()
	var nodes []*model.SyncNode
	err := db.Model(model.SyncNode{}).Find(&nodes).Error
	return nodes, err
}

// GetEnabledSyncNodes returns only enabled sync nodes.
func (s *SyncService) GetEnabledSyncNodes() ([]*model.SyncNode, error) {
	db := database.GetDB()
	var nodes []*model.SyncNode
	err := db.Model(model.SyncNode{}).Where("enable = ?", true).Find(&nodes).Error
	return nodes, err
}

// AddSyncNode adds a new sync node.
func (s *SyncService) AddSyncNode(node *model.SyncNode) error {
	db := database.GetDB()
	return db.Create(node).Error
}

// UpdateSyncNode updates an existing sync node.
func (s *SyncService) UpdateSyncNode(node *model.SyncNode) error {
	db := database.GetDB()
	return db.Save(node).Error
}

// DelSyncNode deletes a sync node by ID.
func (s *SyncService) DelSyncNode(id int) error {
	db := database.GetDB()
	return db.Delete(model.SyncNode{}, id).Error
}

// IsMaster returns true if this node has any enabled sync nodes configured (acts as master).
func (s *SyncService) IsMaster() bool {
	enabled, err := s.settingService.GetSyncEnable()
	if err != nil || !enabled {
		return false
	}
	nodes, err := s.GetEnabledSyncNodes()
	if err != nil {
		return false
	}
	return len(nodes) > 0
}

// BuildSyncPayload constructs the full sync payload from the master's database.
func (s *SyncService) BuildSyncPayload() (*SyncPayload, error) {
	inbounds, err := s.inboundService.GetAllInbounds()
	if err != nil {
		return nil, fmt.Errorf("build sync payload: get inbounds: %w", err)
	}

	// Collect all client traffics from DB
	db := database.GetDB()
	var traffics []xray.ClientTraffic
	if err := db.Model(xray.ClientTraffic{}).Find(&traffics).Error; err != nil {
		return nil, fmt.Errorf("build sync payload: get traffics: %w", err)
	}

	// Build settings snapshot
	settings, err := s.buildSyncSettings()
	if err != nil {
		logger.Warning("build sync settings failed:", err)
		// non-fatal, continue without settings
	}

	masterURL, _ := s.settingService.GetSyncMasterURL()

	// Strip ClientStats from inbounds to avoid duplicate traffic data
	// (traffics field carries the authoritative traffic state)
	inboundsClean := make([]model.Inbound, len(inbounds))
	for i, ib := range inbounds {
		inboundsClean[i] = *ib
		inboundsClean[i].ClientStats = nil
	}

	return &SyncPayload{
		Timestamp: time.Now().UnixMilli(),
		Inbounds:  inboundsClean,
		Traffics:  traffics,
		Settings:  settings,
		MasterURL: strings.TrimRight(masterURL, "/"),
	}, nil
}

func (s *SyncService) buildSyncSettings() (*SyncSettings, error) {
	xrayTemplate, err := s.settingService.GetXrayConfigTemplate()
	if err != nil {
		return nil, err
	}
	subPath, _ := s.settingService.GetSubPath()
	subTitle, _ := s.settingService.GetSubTitle()
	subEnableRouting, _ := s.settingService.GetSubEnableRouting()
	subRoutingRules, _ := s.settingService.GetSubRoutingRules()
	subJsonFragment, _ := s.settingService.GetSubJsonFragment()
	subJsonNoises, _ := s.settingService.GetSubJsonNoises()
	subJsonMux, _ := s.settingService.GetSubJsonMux()
	subJsonRules, _ := s.settingService.GetSubJsonRules()

	return &SyncSettings{
		XrayTemplateConfig: xrayTemplate,
		SubPath:            subPath,
		SubTitle:           subTitle,
		SubEnableRouting:   subEnableRouting,
		SubRoutingRules:    subRoutingRules,
		SubJsonFragment:    subJsonFragment,
		SubJsonNoises:      subJsonNoises,
		SubJsonMux:         subJsonMux,
		SubJsonRules:       subJsonRules,
	}, nil
}

// TriggerSync asynchronously pushes the current config to all enabled slave nodes.
// Called after every inbound/client mutation on the master.
func (s *SyncService) TriggerSync() {
	if !s.IsMaster() {
		return
	}
	payload, err := s.BuildSyncPayload()
	if err != nil {
		logger.Warning("sync trigger: build payload failed:", err)
		return
	}
	nodes, err := s.GetEnabledSyncNodes()
	if err != nil {
		logger.Warning("sync trigger: get nodes failed:", err)
		return
	}
	for _, node := range nodes {
		go func(n *model.SyncNode) {
			if err := s.pushToNode(n, payload); err != nil {
				logger.Warningf("sync push to %s (%s) failed: %v", n.Name, n.Url, err)
			}
		}(node)
	}
}

// FullSync performs a full synchronization to all enabled slave nodes.
// Called by the periodic sync job as a compensating mechanism.
func (s *SyncService) FullSync() {
	if !s.IsMaster() {
		return
	}
	payload, err := s.BuildSyncPayload()
	if err != nil {
		logger.Warning("full sync: build payload failed:", err)
		return
	}
	nodes, err := s.GetEnabledSyncNodes()
	if err != nil {
		logger.Warning("full sync: get nodes failed:", err)
		return
	}
	for _, node := range nodes {
		if err := s.pushToNode(node, payload); err != nil {
			logger.Warningf("full sync push to %s (%s) failed: %v", node.Name, node.Url, err)
		}
	}
}

// pushToNode sends the sync payload to a single slave node, authenticating with the node's own secret.
func (s *SyncService) pushToNode(node *model.SyncNode, payload *SyncPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	url := strings.TrimRight(node.Url, "/") + "/sync/push"
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Sync-Secret", node.Secret)

	resp, err := syncHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

// TestNodeConnection attempts a push to a node and returns error if unreachable or auth fails.
func (s *SyncService) TestNodeConnection(nodeId int) error {
	db := database.GetDB()
	var node model.SyncNode
	if err := db.First(&node, nodeId).Error; err != nil {
		return fmt.Errorf("node not found: %w", err)
	}
	// Send an empty payload just to test connectivity and auth
	testPayload := &SyncPayload{Timestamp: time.Now().UnixMilli()}
	return s.pushToNode(&node, testPayload)
}

// ValidateNodeSecret checks whether the given secret belongs to any enabled slave node.
// Used by the master to authenticate incoming traffic reports.
func (s *SyncService) ValidateNodeSecret(secret string) bool {
	if secret == "" {
		return false
	}
	db := database.GetDB()
	var count int64
	db.Model(model.SyncNode{}).Where("secret = ? AND enable = ?", secret, true).Count(&count)
	return count > 0
}

// ApplySyncPayload applies a received sync payload on a slave node.
// Inbounds are reconciled by Tag; client traffics are overwritten with master's aggregated values.
func (s *SyncService) ApplySyncPayload(payload *SyncPayload) error {
	db := database.GetDB()

	// Get first user ID for assigning to new inbounds
	var firstUser model.User
	if err := db.First(&firstUser).Error; err != nil {
		return fmt.Errorf("get user: %w", err)
	}

	// Build map of existing inbounds by Tag
	var existingInbounds []model.Inbound
	if err := db.Model(model.Inbound{}).Find(&existingInbounds).Error; err != nil {
		return fmt.Errorf("get existing inbounds: %w", err)
	}
	tagToInbound := make(map[string]*model.Inbound, len(existingInbounds))
	for i := range existingInbounds {
		tagToInbound[existingInbounds[i].Tag] = &existingInbounds[i]
	}

	needRestart := false

	for _, ib := range payload.Inbounds {
		existing, exists := tagToInbound[ib.Tag]
		if exists {
			// Only update and restart if Xray-affecting fields actually changed
			changed := existing.Enable != ib.Enable ||
				existing.Listen != ib.Listen ||
				existing.Port != ib.Port ||
				existing.Protocol != ib.Protocol ||
				existing.Settings != ib.Settings ||
				existing.StreamSettings != ib.StreamSettings ||
				existing.Sniffing != ib.Sniffing
			existing.Remark = ib.Remark
			existing.Enable = ib.Enable
			existing.ExpiryTime = ib.ExpiryTime
			existing.Total = ib.Total
			existing.TrafficReset = ib.TrafficReset
			existing.Listen = ib.Listen
			existing.Port = ib.Port
			existing.Protocol = ib.Protocol
			existing.Settings = ib.Settings
			existing.StreamSettings = ib.StreamSettings
			existing.Sniffing = ib.Sniffing
			if err := db.Save(existing).Error; err != nil {
				logger.Warningf("sync apply: update inbound tag=%s: %v", ib.Tag, err)
			}
			if changed {
				needRestart = true
			}
		} else {
			// Insert new inbound
			newIb := ib
			newIb.Id = 0
			newIb.UserId = firstUser.Id
			newIb.Up = 0
			newIb.Down = 0
			newIb.AllTime = 0
			newIb.ClientStats = nil
			if err := db.Create(&newIb).Error; err != nil {
				logger.Warningf("sync apply: create inbound tag=%s: %v", ib.Tag, err)
			}
			needRestart = true
		}
	}

	// Reconcile client traffics: overwrite with master's aggregated values
	for _, ct := range payload.Traffics {
		var existing xray.ClientTraffic
		result := db.Model(xray.ClientTraffic{}).Where("email = ?", ct.Email).First(&existing)
		if database.IsNotFound(result.Error) {
			// Create new traffic record (inbound may have just been created)
			newCt := ct
			newCt.Id = 0
			db.Create(&newCt)
		} else if result.Error == nil {
			// Overwrite with master's aggregated values
			existing.Enable = ct.Enable
			existing.Total = ct.Total
			existing.ExpiryTime = ct.ExpiryTime
			existing.Up = ct.Up
			existing.Down = ct.Down
			existing.Reset = ct.Reset
			db.Save(&existing)
		}
	}

	// Apply synced settings if present; XrayTemplateConfig changes require restart
	if payload.Settings != nil {
		if s.applySyncSettings(payload.Settings) {
			needRestart = true
		}
	}

	// Auto-configure traffic reporting URI so slaves can report back without manual setup.
	// If master provided its public URL, build the URI using this slave's own syncSecret.
	if payload.MasterURL != "" {
		slaveSecret, err := s.settingService.GetSyncSecret()
		if err == nil && slaveSecret != "" {
			informURI := payload.MasterURL + "/sync/traffic?secret=" + slaveSecret
			_ = s.settingService.SetExternalTrafficInformEnable(true)
			_ = s.settingService.SetExternalTrafficInformURI(informURI)
		}
	}

	if needRestart {
		go func() {
			if err := s.xrayService.RestartXray(true); err != nil {
				logger.Warning("sync apply: restart xray failed:", err)
			}
		}()
	}

	return nil
}

// applySyncSettings applies received settings on the slave node.
// Returns true if XrayTemplateConfig was changed (requires Xray restart).
func (s *SyncService) applySyncSettings(settings *SyncSettings) bool {
	xrayConfigChanged := false
	if settings.XrayTemplateConfig != "" {
		if err := s.settingService.SetXrayConfigTemplate(settings.XrayTemplateConfig); err != nil {
			logger.Warning("sync apply settings: set xray template:", err)
		} else {
			xrayConfigChanged = true
		}
	}
	if settings.SubPath != "" {
		s.settingService.SetSubPath(settings.SubPath)
	}
	if settings.SubTitle != "" {
		s.settingService.SetSubTitle(settings.SubTitle)
	}
	s.settingService.SetSubEnableRouting(settings.SubEnableRouting)
	if settings.SubRoutingRules != "" {
		s.settingService.SetSubRoutingRules(settings.SubRoutingRules)
	}
	if settings.SubJsonFragment != "" {
		s.settingService.SetSubJsonFragment(settings.SubJsonFragment)
	}
	if settings.SubJsonNoises != "" {
		s.settingService.SetSubJsonNoises(settings.SubJsonNoises)
	}
	if settings.SubJsonMux != "" {
		s.settingService.SetSubJsonMux(settings.SubJsonMux)
	}
	if settings.SubJsonRules != "" {
		s.settingService.SetSubJsonRules(settings.SubJsonRules)
	}
	return xrayConfigChanged
}

// HandleTrafficReport accumulates incremental traffic deltas from a slave node
// into the master's ClientTraffic records. The same format as ExternalTrafficInformURI is used.
func (s *SyncService) HandleTrafficReport(delta *TrafficDelta) error {
	if len(delta.ClientTraffics) == 0 {
		return nil
	}
	db := database.GetDB()
	for _, ct := range delta.ClientTraffics {
		if ct.Email == "" {
			continue
		}
		result := db.Model(xray.ClientTraffic{}).
			Where("email = ?", ct.Email).
			Updates(map[string]any{
				"up":   gorm.Expr("up + ?", ct.Up),
				"down": gorm.Expr("down + ?", ct.Down),
			})
		if result.Error != nil {
			logger.Warningf("traffic report: update %s: %v", ct.Email, result.Error)
		}
	}
	return nil
}
