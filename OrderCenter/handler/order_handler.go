package handler

import (
	"net/http"
	"trade-solution/ordercenter/model"
	"trade-solution/ordercenter/service"

	"github.com/gin-gonic/gin"
)

func RegisterOrderRoutes(r *gin.Engine, srv *service.OrderService) {
	group := r.Group("/orders")

	group.POST("", func(c *gin.Context) {
		var order model.OrderData
		if err := c.ShouldBindJSON(&order); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := srv.CreateOrder(&order); err != nil {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "订单创建成功"})
	})

	group.GET("/:id", func(c *gin.Context) {
		id := c.Param("id")
		order, err := srv.GetOrderByID(id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, order)
	})

	group.PUT("/:id", func(c *gin.Context) {
		id := c.Param("id")
		var updated model.OrderData
		if err := c.ShouldBindJSON(&updated); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if err := srv.UpdateOrder(id, &updated); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "订单更新成功"})
	})

	group.DELETE("/:id", func(c *gin.Context) {
		id := c.Param("id")
		if err := srv.DeleteOrder(id); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"message": "订单删除成功"})
	})

	group.GET("", func(c *gin.Context) {
		orders, err := srv.GetAllOrders()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, orders)
	})
}
