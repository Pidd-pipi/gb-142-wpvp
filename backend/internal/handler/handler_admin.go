package handler

import (
	"strconv"

	"github.com/blueship581/gbcarenotify/internal/dto"
	"github.com/blueship581/gbcarenotify/internal/service"
	"github.com/gin-gonic/gin"
)

type AdminHandler struct {
	*Handler
	service   *service.StatusService
	occupancy *service.OccupancyService
}

func NewAdminHandler(base *Handler, service *service.StatusService, occupancy *service.OccupancyService) *AdminHandler {
	return &AdminHandler{Handler: base, service: service, occupancy: occupancy}
}
func (h *AdminHandler) Statuses(c *gin.Context) {
	page, size := pagination(c)
	items, total, err := h.service.List(c.Request.Context(), page, size)
	if err != nil {
		respondError(c, err)
		return
	}
	ok(c, gin.H{"items": items, "pagination": dto.Pagination{Page: page, PageSize: size, Total: total}})
}

// Occupancies reads back greeting periods / alert events with their status and
// successful-log linkage. Optional filters: recipient_id, kind.
func (h *AdminHandler) Occupancies(c *gin.Context) {
	page, size := pagination(c)
	var recipientID *uint
	if raw := c.Query("recipient_id"); raw != "" {
		id, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || id == 0 {
			c.JSON(400, dto.APIResponse{Code: 40003, Message: "invalid recipient_id"})
			return
		}
		parsed := uint(id)
		recipientID = &parsed
	}
	kind := c.Query("kind")
	items, total, err := h.occupancy.List(c.Request.Context(), page, size, recipientID, kind)
	if err != nil {
		respondError(c, err)
		return
	}
	ok(c, gin.H{"items": items, "pagination": dto.Pagination{Page: page, PageSize: size, Total: total}})
}
