package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ================= GLOBALS =================
var db *sql.DB
var rdb *redis.Client
var workerPool = make(chan struct{}, 100)

// ================= STRUCTS =================
type Contact struct {
	Names       map[string]int `json:"names"`
	SpamReports int            `json:"spam_reports"`
	IsBusiness  bool           `json:"is_business"`
}

type ContactItem struct {
	Name   string `json:"name"`
	Number string `json:"number"`
}

// ================= INIT DB & REDIS =================
func initDB() {
	connStr := "host=" + os.Getenv("PGHOST") +
		" port=" + os.Getenv("PGPORT") +
		" user=" + os.Getenv("PGUSER") +
		" password=" + os.Getenv("PGPASSWORD") +
		" dbname=" + os.Getenv("PGDATABASE") +
		" sslmode=disable"

	var err error
	db, err = sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}

	if err := db.Ping(); err != nil {
		log.Fatal("DB not connected:", err)
	}

	// Create table if not exists
	db.Exec(`
	CREATE TABLE IF NOT EXISTS contacts (
		number TEXT PRIMARY KEY,
		names JSONB,
		spam_reports INT DEFAULT 0,
		is_business BOOLEAN DEFAULT FALSE,
		updated_at TIMESTAMP DEFAULT NOW()
	);`)

	// Redis init
	rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv("REDIS_HOST"),
		Password: os.Getenv("REDIS_PASSWORD"),
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		log.Fatal("Redis not connected:", err)
	}
}

// ================= HELPERS =================
func normalizeNumber(number string) string {
	digits := ""
	for _, c := range number {
		if c >= '0' && c <= '9' {
			digits += string(c)
		}
	}
	if len(digits) >= 10 {
		return digits[len(digits)-10:]
	}
	return digits
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
func getFromCache(number string) (Contact, bool) {
	val, err := rdb.Get(context.Background(), "c:"+number).Result()
	if err != nil {
		return Contact{}, false
	}
	var data Contact
	json.Unmarshal([]byte(val), &data)
	return data, true
}

func setCache(number string, data Contact) {
	j, _ := json.Marshal(data)
	rdb.Set(context.Background(), "c:"+number, j, 10*time.Minute)
}

// ================= DB =================
func getFromDB(number string) (Contact, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	row := db.QueryRowContext(ctx,
		"SELECT names, spam_reports, is_business FROM contacts WHERE number=$1", number)

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

// ================= UPSERT =================
func upsertContact(name, num string, isBiz bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := db.ExecContext(ctx, `
	INSERT INTO contacts (number, names, is_business)
	VALUES ($1, jsonb_build_object($2::text, 1), $3)
	ON CONFLICT (number)
	DO UPDATE SET 
	names = contacts.names || 
	        jsonb_build_object(
	            $2::text, 
	            COALESCE((contacts.names ->> $2)::int, 0) + 1
	        ),
	is_business = contacts.is_business OR $3,
	updated_at = NOW()
	`, num, name, isBiz)

	return err
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

	isSpam := data.SpamReports > 5

	return gin.H{
		"name":            top,
		"number":          num,
		"clean_number":    num,
		"is_spam":         isSpam,
		"location":        "India",
		"votes":           total,
		"is_business":     data.IsBusiness,
		"spam_reports":    data.SpamReports,
		"status":          "success",
		"all_suggestions": data.Names,
	}
}

// ================= GET CALLER =================
func getCaller(c *gin.Context) {
	num := normalizeNumber(c.Query("number"))

	if data, ok := getFromCache(num); ok {
		c.JSON(200, buildResponse(num, data))
		return
	}

	data, ok := getFromDB(num)
	if !ok {
		c.JSON(200, gin.H{
			"name":         "Unknown",
			"number":       num,
			"clean_number": num,
			"status":       "not_found",
		})
		return
	}

	setCache(num, data)
	c.JSON(200, buildResponse(num, data))
}

// ================= SYNC CONTACTS =================
func syncContacts(c *gin.Context) {
	var req struct {
		Contacts []ContactItem `json:"contacts"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		log.Println("❌ JSON ERROR:", err)
		c.JSON(400, gin.H{"status": "invalid"})
		return
	}

	var wg sync.WaitGroup
	var processed int64

	for _, ct := range req.Contacts {
		workerPool <- struct{}{}
		wg.Add(1)

		go func(cn ContactItem) {
			defer wg.Done()
			defer func() { <-workerPool }()

			num := normalizeNumber(cn.Number)
			name := strings.TrimSpace(cn.Name)

			if num == "" || name == "" {
				return
			}

			if err := upsertContact(name, num, isBusinessName(name)); err != nil {
				log.Println("❌ DB ERROR:", err)
				return
			}

			data, _ := getFromDB(num)
			setCache(num, data)

			atomic.AddInt64(&processed, 1)
		}(ct)
	}

	wg.Wait()

	c.JSON(200, gin.H{
		"processed": processed,
		"status":    "success",
	})
}

// ================= REPORT SPAM =================
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

// ================= ADMIN =================
func adminPage(c *gin.Context) {
	c.File("./static/admin.html")
}

func adminSearch(c *gin.Context) {
	num := normalizeNumber(c.Query("number"))

	row := db.QueryRow("SELECT names, spam_reports, is_business FROM contacts WHERE number=$1", num)

	var namesJSON []byte
	var spam int
	var isBiz bool

	err := row.Scan(&namesJSON, &spam, &isBiz)
	if err != nil {
		c.JSON(200, gin.H{"status": "not_found"})
		return
	}

	names := make(map[string]int)
	json.Unmarshal(namesJSON, &names)

	c.JSON(200, gin.H{
		"number": num,
		"names":  names,
		"spam":   spam,
		"isBiz":  isBiz,
	})
}

func adminUpload(c *gin.Context) {
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(400, gin.H{"error": "file missing"})
		return
	}

	path := "./admin/" + file.Filename
	c.SaveUploadedFile(file, path)

	data, _ := os.ReadFile(path)

	var req struct {
		Contacts []ContactItem `json:"contacts"`
	}

	json.Unmarshal(data, &req)

	count := 0

	for _, ct := range req.Contacts {
		num := normalizeNumber(ct.Number)
		name := strings.TrimSpace(ct.Name)

		if num == "" || name == "" {
			continue
		}

		db.Exec(`
		INSERT INTO contacts (number, names)
		VALUES ($1, jsonb_build_object($2::text, 1))
		ON CONFLICT (number)
		DO UPDATE SET 
		names = contacts.names || jsonb_build_object(
			$2::text,
			COALESCE((contacts.names ->> $2)::int, 0) + 1
		)
		`, num, name)

		count++
	}

	c.JSON(200, gin.H{"uploaded": count})
}

// ================= MAIN =================
func main() {
	initDB()

	r := gin.Default()
	r.SetTrustedProxies(nil)

	// Public routes
	r.GET("/get", getCaller)

	// API routes
	api := r.Group("/api")
	api.Use(func(c *gin.Context) {
		if c.GetHeader("X-API-Key") != "truecaller_pro_2026" {
			c.JSON(401, gin.H{"error": "unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	})
	api.POST("/sync", syncContacts)
	api.POST("/report_spam", reportSpam)

	// Admin routes
	r.GET("/admin", adminPage)
	r.GET("/admin/search", adminSearch)
	r.POST("/admin/upload", adminUpload)

	// Health check
	r.GET("/health", func(c *gin.Context) {
		c.String(200, "OK")
	})

	log.Println("🚀 Running at port", os.Getenv("PORT"))
	port := os.Getenv("PORT")
	if port == "" {
		port = "5001"
	}
	r.Run(":" + port)
}
