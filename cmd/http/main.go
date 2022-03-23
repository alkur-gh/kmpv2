package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"

	api "github.com/alkur-gh/kmpv2/api/v1"
	"github.com/alkur-gh/kmpv2/internal/replicator"
	"github.com/alkur-gh/kmpv2/internal/storage/inmemory"
	"github.com/gin-gonic/gin"
    "github.com/gin-contrib/cors"
)

var idp = flag.String("id", "node", "node id")
var baseDirp = flag.String("basedir", "", "base dir for data")
var raftBindAddrp = flag.String("raft_addr", "127.0.0.1:8001", "raft address")
var serfBindAddrp = flag.String("serf_addr", "127.0.0.1:8002", "serf address")
var httpBindAddrp = flag.String("http_addr", "127.0.0.1:8080", "http address")
var bootstrapp = flag.Bool("bootstrap", false, "bootstrap")

func parseFlags() {
	flag.Parse()
	if *baseDirp == "" {
		dir, err := os.MkdirTemp("", "replicator-test-*")
		if err != nil {
			log.Fatalf("os.CreateTemp() error: %v", err)
		}
		err = os.Mkdir(filepath.Join(dir, "raft"), 0777)
		if err != nil {
			log.Fatalf("error: %v", err)
		}
		*baseDirp = dir
	}
}

func main() {
	parseFlags()

	storage, err := inmemory.New(log.New(os.Stdout, "[storage] ", log.Flags()))
	if err != nil {
		log.Fatalf("failed to create storage: %v", err)
	}
	repl, err := replicator.New(replicator.Config{
		ID:           *idp,
		BaseDir:      *baseDirp,
		RaftBindAddr: *raftBindAddrp,
		SerfBindAddr: *serfBindAddrp,
		Bootstrap:    *bootstrapp,
		Storage:      storage,
	})
	_ = repl
	if err != nil {
		log.Fatalf("failed to create replicator: %v", err)
	}

	r := gin.Default()

    r.Use(cors.Default())

	r.GET("/", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, gin.H{
			"message": "hello, world!",
		})
	})

	r.GET("/status/members", func(c *gin.Context) {
		members, err := repl.Members()
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, members)
	})

	r.GET("/status/leader", func(c *gin.Context) {
		leader, err := repl.Leader()
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, gin.H{
			"address": string(leader),
		})
	})

	r.Any("/control/join", func(c *gin.Context) {
		var data struct {
			Address string `json:"address"`
		}
		if err := c.BindJSON(&data); err != nil {
			return
		}
		if err := repl.Join([]string{data.Address}); err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.Status(http.StatusOK)
	})

	r.Any("/control/leave", func(c *gin.Context) {
		if err := repl.Leave(); err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.Status(http.StatusOK)
	})

	r.GET("/records", func(c *gin.Context) {
		rr, err := storage.GetAll()
		if err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.IndentedJSON(http.StatusOK, rr)
	})

	r.GET("/records/:key", func(c *gin.Context) {
		key := c.Param("key")
		rec, err := storage.Get(key)
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
		if err := repl.Put(&rec); err != nil {
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
		if err := repl.Delete(key); err != nil {
			c.IndentedJSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}
		c.Status(http.StatusOK)
	})

	r.Run(*httpBindAddrp)
}
