package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"github.com/alkur-gh/kmpv2/internal/manager"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/phayes/freeport"
)

var hostname = flag.String("hostname", "127.0.0.1", "default hostname")
var httpAddr = flag.String("http_addr", "0.0.0.0:8080", "http address")

func freeAddress() (string, error) {
	port, err := freeport.GetFreePort()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", *hostname, port), nil
}

func main() {
	flag.Parse()
	m, err := manager.New()
	if err != nil {
		log.Fatalf("failed to create replication manager: %v", err)
	}
	r := gin.Default()
	r.Use(cors.Default())

	r.GET("/status", func(c *gin.Context) {
		info, err := m.StatusInfo()
		if err != nil {
			status := http.StatusInternalServerError
			// dont know correct error code for ErrNotStarted
			c.IndentedJSON(status, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, &info)
	})

	r.Any("/control/start", func(c *gin.Context) {
		var config manager.Config
		if err := c.BindJSON(&config); err != nil {
			return
		}
		if config.ID == "" || !config.Bootstrap && len(config.JoinAddrs) == 0 {
			c.Status(http.StatusBadRequest)
			return
		}
		var err error
		if config.SerfAddr == "" {
			config.SerfAddr, err = freeAddress()
		}
		if err == nil && config.RaftAddr == "" {
			config.RaftAddr, err = freeAddress()
		}
		if err == nil {
			err = m.Start(config)
		}
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
		}
		c.Status(http.StatusOK)
	})

	r.Any("/control/stop", func(c *gin.Context) {
		if err := m.Stop(); err != nil {
			status := http.StatusInternalServerError
			c.IndentedJSON(status, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.Status(http.StatusOK)
	})

	r.GET("/records", func(c *gin.Context) {
		rr, err := m.GetAll()
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		if rr == nil {
			rr = []*api.Record{}
		}
		c.IndentedJSON(http.StatusOK, rr)
	})

	r.GET("/records/:key", func(c *gin.Context) {
		key := c.Param("key")
		rec, err := m.Get(key)
		if err != nil {
			status := http.StatusInternalServerError
			if _, ok := err.(api.ErrRecordNotFound); ok {
				status = http.StatusNotFound
			}
			c.IndentedJSON(status, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, rec)
	})

	r.PUT("/records/:key", func(c *gin.Context) {
		var rec api.Record
		key := c.Param("key")
		if err := c.BindJSON(&rec); err != nil {
			return
		}
		rec.Key = key
		if err := m.Put(&rec); err != nil {
			status := http.StatusInternalServerError
			c.IndentedJSON(status, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, &rec)
	})

	r.DELETE("/records/:key", func(c *gin.Context) {
		key := c.Param("key")
		if err := m.Delete(key); err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.Status(http.StatusOK)
	})

	r.Run(*httpAddr)
}
