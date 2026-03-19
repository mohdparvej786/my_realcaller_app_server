package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var db *sql.DB
var rdb *redis.Client
var ctx = context.Background()

// 🚀 RAM Cache
var localCache sync.Map

// ================= STRUCT =================
type Contact struct {
	Names       map[string]int `json:"names"`
	SpamReports int            `json:"spam_reports"`
	IsBusiness  bool           `json:"is_business"`
}

type ContactInput struct {
	Name   string `json:"name"`
	Number string `json:"number"`
}

// ================= INIT =================
func initDB() {
	connStr := os.Getenv("DATABASE_URL")

	var err error
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// 🚀 Pooling
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal("DB error:", err)
	}

	db.Exec(`
	CREATE TABLE IF NOT EXISTS contacts (
		number TEXT PRIMARY KEY,
		names JSONB,
		spam_reports INT DEFAULT 0,
		is_business BOOLEAN DEFAULT FALSE,
		updated_at TIMESTAMP DEFAULT NOW()
	);`)

	db.Exec(`CREATE INDEX IF NOT EXISTS idx_number ON contacts(number);`)
}

// ================= REDIS =================
func initRedis() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Println("⚠️ Redis disabled")
		return
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return
	}

	rdb = redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb = nil
		return
	}

	log.Println("✅ Redis connected")
}

// ================= HELPERS =================
func normalizeNumber(number string) string {
	var digits strings.Builder
	for _, c := range number {
		if c >= '0' && c <= '9' {
			digits.WriteRune(c)
		}
	}
	num := digits.String()
	if len(num) >= 10 {
		return num[len(num)-10:]
	}
	return num
}

func isBusinessName(name string) bool {
	keywords := []string{"shop", "store", "clinic", "hospital", "bank", "service", "pvt", "ltd"}
	name = strings.ToLower(name)
	for _, k := range keywords {
		if strings.Contains(name, k) {
			return true
		}
	}
	return false
}

// ================= CACHE =================

// RAM
func getLocal(num string) (Contact, bool) {
	val, ok := localCache.Load(num)
	if !ok {
		return Contact{}, false
	}
	return val.(Contact), true
}

func setLocal(num string, data Contact) {
	localCache.Store(num, data)
}

// Redis
func getRedis(num string) (Contact, bool) {
	if rdb == nil {
		return Contact{}, false
	}
	val, err := rdb.Get(ctx, "c:"+num).Result()
	if err != nil {
		return Contact{}, false
	}
	var data Contact
	json.Unmarshal([]byte(val), &data)
	return data, true
}

func setRedis(num string, data Contact) {
	if rdb == nil {
		return
	}
	j, _ := json.Marshal(data)
	rdb.Set(ctx, "c:"+num, j, 1*time.Hour)
}

// ================= DB =================
func getFromDB(num string) (Contact, bool) {
	ctxDB, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	row := db.QueryRowContext(ctxDB,
		"SELECT names, spam_reports, is_business FROM contacts WHERE number=$1", num)

	var namesJSON []byte
	var spam int
	var isBiz bool

	err := row.Scan(&namesJSON, &spam, &isBiz)
	if err != nil {
		return Contact{}, false
	}

	names := make(map[string]int)
	json.Unmarshal(namesJSON, &names)

	return Contact{names, spam, isBiz}, true
}

// ================= SPAM LOGIC =================
func isSpamAdvanced(data Contact) bool {
	total := 0
	max := 0

	for _, v := range data.Names {
		total += v
		if v > max {
			max = v
		}
	}

	if data.SpamReports > 10 {
		return true
	}

	if total > 20 && max < (total/2) {
		return true
	}

	return false
}

// ================= RESPONSE =================
func buildResponse(num string, data Contact) gin.H {
	top := "Unknown"
	max := 0
	total := 0

	for n, c := range data.Names {
		total += c
		if c > max {
			max = c
			top = n
		}
	}

	return gin.H{
		"name":         top,
		"number":       num,
		"is_spam":      isSpamAdvanced(data),
		"votes":        total,
		"is_business":  data.IsBusiness,
		"spam_reports": data.SpamReports,
		"status":       "success",
	}
}

// ================= GET =================
func getCaller(c *gin.Context) {
	num := normalizeNumber(c.Query("number"))

	// 1. RAM
	if data, ok := getLocal(num); ok {
		c.JSON(200, buildResponse(num, data))
		return
	}

	// 2. Redis
	if data, ok := getRedis(num); ok {
		setLocal(num, data)
		c.JSON(200, buildResponse(num, data))
		return
	}

	// 3. DB
	data, ok := getFromDB(num)
	if !ok {
		empty := Contact{}
		setLocal(num, empty)
		setRedis(num, empty)

		c.JSON(200, gin.H{
			"name":   "Unknown",
			"number": num,
			"status": "not_found",
		})
		return
	}

	setLocal(num, data)
	setRedis(num, data)

	c.JSON(200, buildResponse(num, data))
}

// ================= FAST SYNC =================
func syncContactsFast(c *gin.Context) {
	var req struct {
		Contacts []ContactInput `json:"contacts"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"status": "invalid"})
		return
	}

	tx, _ := db.Begin()
	stmt, _ := tx.Prepare(`
	INSERT INTO contacts (number, names, is_business)
	VALUES ($1, jsonb_build_object($2::text, 1), $3)
	ON CONFLICT (number)
	DO UPDATE SET 
	names = contacts.names || jsonb_build_object(
		$2::text,
		COALESCE((contacts.names ->> $2)::int, 0) + 1
	),
	is_business = contacts.is_business OR $3
	`)

	count := 0

	for _, ct := range req.Contacts {
		num := normalizeNumber(ct.Number)
		name := strings.TrimSpace(ct.Name)

		if num == "" || name == "" {
			continue
		}

		stmt.Exec(num, name, isBusinessName(name))
		count++
	}

	stmt.Close()
	tx.Commit()

	c.JSON(200, gin.H{"processed": count})
}

// ================= SPAM =================
func reportSpam(c *gin.Context) {
	var req struct {
		Number string `json:"number"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(400, gin.H{"status": "invalid"})
		return
	}

	num := normalizeNumber(req.Number)

	db.Exec(`UPDATE contacts SET spam_reports = spam_reports + 1 WHERE number=$1`, num)

	c.JSON(200, gin.H{"status": "reported"})
}

// ================= MAIN =================
func main() {
	initDB()
	initRedis()

	r := gin.Default()

	r.GET("/get", getCaller)

	api := r.Group("/api")
	api.Use(func(c *gin.Context) {
		if c.GetHeader("X-API-Key") != "truecaller_pro_2026" {
			c.JSON(401, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	})

	api.POST("/sync", syncContactsFast)
	api.POST("/report_spam", reportSpam)

	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "5001"
	}

	log.Println("🚀 Production Server Running:", port)
	r.Run(":" + port)
}
