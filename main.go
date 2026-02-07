package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

//go:embed web
var webFS embed.FS

// ── Data Models ──

type Work struct {
	ID             string   `json:"id"`
	Title          string   `json:"title"`
	Circle         string   `json:"circle"`
	CircleID       string   `json:"circle_id"`
	Description    string   `json:"description"`
	ImageURL       string   `json:"image_url"`
	SampleImages   []string `json:"sample_images"`
	Price          string   `json:"price"`
	Rating         float64  `json:"rating"`
	RatingCount    int      `json:"rating_count"`
	DLCount        string   `json:"dl_count"`
	AgeRating      string   `json:"age_rating"`
	WorkType       string   `json:"work_type"`
	FileSize       string   `json:"file_size"`
	ReleaseDate    string   `json:"release_date"`
	UpdateDate     string   `json:"update_date"`
	Series         string   `json:"series"`
	Genres         []string `json:"genres"`
	FileFormats    []string `json:"file_formats"`
	PageURL        string   `json:"page_url"`
	Status         string   `json:"status"`
	Category       string   `json:"category"`
	FetchedAt      string   `json:"fetched_at"`
	SiteCategory   string   `json:"site_category"`
}

type ScrapeTask struct {
	StartID    int    `json:"start_id"`
	EndID      int    `json:"end_id"`
	Prefix     string `json:"prefix"`
	Category   string `json:"site_category"`
	DelayMs    int    `json:"delay_ms"`
}

type ScrapeStatus struct {
	Running     bool   `json:"running"`
	CurrentID   string `json:"current_id"`
	TotalDone   int    `json:"total_done"`
	TotalFound  int    `json:"total_found"`
	TotalErrors int    `json:"total_errors"`
	Speed       string `json:"speed"`
	StartedAt   string `json:"started_at"`
	Message     string `json:"message"`
}

// ── Global State ──

var (
	db           *sql.DB
	scrapeCtx    context.Context
	scrapeCancel context.CancelFunc
	scrapeMu     sync.Mutex
	scrapeStatus = ScrapeStatus{Message: "空闲"}
	httpClient   = &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 3 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
	workIDRegex = regexp.MustCompile(`(RJ|RE|BJ|VJ)(\d{6,8})`)
)

// ── Database ──

func initDB() error {
	var err error
	dbPath := "dlsite.db"
	if v := os.Getenv("DLSITE_DB"); v != "" {
		dbPath = v
	}
	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		return err
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA cache_size=5000",
		"PRAGMA busy_timeout=5000",
	}
	for _, p := range pragmas {
		db.Exec(p)
	}

	schema := `
	CREATE TABLE IF NOT EXISTS works (
		id TEXT PRIMARY KEY,
		title TEXT NOT NULL DEFAULT '',
		circle TEXT DEFAULT '',
		circle_id TEXT DEFAULT '',
		description TEXT DEFAULT '',
		image_url TEXT DEFAULT '',
		sample_images TEXT DEFAULT '[]',
		price TEXT DEFAULT '',
		rating REAL DEFAULT 0,
		rating_count INTEGER DEFAULT 0,
		dl_count TEXT DEFAULT '',
		age_rating TEXT DEFAULT '',
		work_type TEXT DEFAULT '',
		file_size TEXT DEFAULT '',
		release_date TEXT DEFAULT '',
		update_date TEXT DEFAULT '',
		series TEXT DEFAULT '',
		genres TEXT DEFAULT '[]',
		file_formats TEXT DEFAULT '[]',
		page_url TEXT DEFAULT '',
		status TEXT DEFAULT 'normal',
		category TEXT DEFAULT '',
		fetched_at TEXT DEFAULT '',
		site_category TEXT DEFAULT 'maniax'
	);
	CREATE INDEX IF NOT EXISTS idx_works_status ON works(status);
	CREATE INDEX IF NOT EXISTS idx_works_category ON works(category);
	CREATE INDEX IF NOT EXISTS idx_works_release ON works(release_date);
	CREATE INDEX IF NOT EXISTS idx_works_rating ON works(rating);
	CREATE INDEX IF NOT EXISTS idx_works_circle ON works(circle);
	CREATE INDEX IF NOT EXISTS idx_works_title ON works(title);

	CREATE TABLE IF NOT EXISTS categories (
		name TEXT PRIMARY KEY,
		color TEXT DEFAULT '#6366f1',
		created_at TEXT DEFAULT (datetime('now'))
	);

	INSERT OR IGNORE INTO categories(name, color) VALUES ('收藏', '#ef4444');
	INSERT OR IGNORE INTO categories(name, color) VALUES ('想玩', '#f59e0b');
	INSERT OR IGNORE INTO categories(name, color) VALUES ('已玩', '#10b981');
	INSERT OR IGNORE INTO categories(name, color) VALUES ('追踪', '#3b82f6');
	`
	_, err = db.Exec(schema)
	return err
}

// ── DLsite Scraper ──

func buildRequest(rawURL string) (*http.Request, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "ja,en;q=0.9,zh;q=0.8")
	req.Header.Set("Referer", "https://www.dlsite.com/")
	req.AddCookie(&http.Cookie{Name: "locale", Value: "ja_JP"})
	req.AddCookie(&http.Cookie{Name: "adultchecked", Value: "1"})
	req.AddCookie(&http.Cookie{Name: "loginchecked", Value: "1"})
	return req, nil
}

type ajaxInfo struct {
	WorkName       string  `json:"work_name"`
	RateAverage    float64 `json:"rate_average_2dp"`
	RateCount      int     `json:"rate_count"`
	AgeCategory    int     `json:"age_category"`
	WorkType       string  `json:"work_type"`
	MakerID        string  `json:"maker_id"`
	SiteID         string  `json:"site_id"`
	RegistDate     string  `json:"regist_date"`
	WorkImage      string  `json:"work_image"`
	DLCount        int     `json:"dl_count"`
	Price          int     `json:"price"`
	PriceStr       string  `json:"price_without_tax"`
}

func fetchAjaxInfo(workID, siteCat string) (*ajaxInfo, error) {
	apiURL := fmt.Sprintf("https://www.dlsite.com/%s/product/info/ajax?product_id=%s&cdn_cache_min=1", siteCat, workID)
	req, err := buildRequest(apiURL)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Referer", fmt.Sprintf("https://www.dlsite.com/%s/work/=/product_id/%s.html", siteCat, workID))

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("not found")
	}
	if resp.StatusCode == 429 {
		return nil, fmt.Errorf("rate limited")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]*ajaxInfo
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	info, ok := result[workID]
	if !ok {
		return nil, fmt.Errorf("no data for %s", workID)
	}
	return info, nil
}

func fetchHTMLPage(workID, siteCat string) (string, error) {
	pageURL := fmt.Sprintf("https://www.dlsite.com/%s/work/=/product_id/%s.html", siteCat, workID)
	req, err := buildRequest(pageURL)
	if err != nil {
		return "", err
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", fmt.Errorf("not found")
	}
	if resp.StatusCode == 429 {
		return "", fmt.Errorf("rate limited")
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func parseHTMLField(html, startMarker, endMarker string) string {
	idx := strings.Index(html, startMarker)
	if idx < 0 {
		return ""
	}
	rest := html[idx+len(startMarker):]
	endIdx := strings.Index(rest, endMarker)
	if endIdx < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:endIdx])
}

func parseTableField(html, header string) string {
	thMarker := ">" + header + "</th>"
	idx := strings.Index(html, thMarker)
	if idx < 0 {
		thMarker = ">" + header + "\n"
		idx = strings.Index(html, thMarker)
		if idx < 0 {
			return ""
		}
	}
	rest := html[idx:]
	tdStart := strings.Index(rest, "<td")
	if tdStart < 0 {
		return ""
	}
	rest = rest[tdStart:]
	gtIdx := strings.Index(rest, ">")
	if gtIdx < 0 {
		return ""
	}
	rest = rest[gtIdx+1:]
	tdEnd := strings.Index(rest, "</td>")
	if tdEnd < 0 {
		return ""
	}
	return strings.TrimSpace(rest[:tdEnd])
}

func extractText(html string) string {
	var result strings.Builder
	inTag := false
	for _, r := range html {
		if r == '<' {
			inTag = true
			continue
		}
		if r == '>' {
			inTag = false
			continue
		}
		if !inTag {
			result.WriteRune(r)
		}
	}
	return strings.TrimSpace(result.String())
}

func extractLinks(html string) []string {
	var results []string
	re := regexp.MustCompile(`<a[^>]*>([^<]+)</a>`)
	matches := re.FindAllStringSubmatch(html, -1)
	for _, m := range matches {
		t := strings.TrimSpace(m[1])
		if t != "" {
			results = append(results, t)
		}
	}
	return results
}

func extractSampleImages(html string) []string {
	var images []string
	re := regexp.MustCompile(`data-src="([^"]+)"`)
	matches := re.FindAllStringSubmatch(html, -1)
	for _, m := range matches {
		u := m[1]
		if strings.Contains(u, "img.dlsite") || strings.Contains(u, "dlsite.jp") {
			if !strings.HasPrefix(u, "http") {
				u = "https:" + u
			}
			images = append(images, u)
		}
	}
	return images
}

func extractDescription(html string) string {
	marker := `itemprop="description"`
	idx := strings.Index(html, marker)
	if idx < 0 {
		marker = `class="work_parts_container"`
		idx = strings.Index(html, marker)
		if idx < 0 {
			return ""
		}
	}
	rest := html[idx:]
	gtIdx := strings.Index(rest, ">")
	if gtIdx < 0 {
		return ""
	}
	rest = rest[gtIdx+1:]

	// Find the closing div, handling nesting
	depth := 1
	endPos := 0
	for i := 0; i < len(rest); i++ {
		if i+4 < len(rest) && rest[i:i+4] == "<div" {
			depth++
		}
		if i+5 < len(rest) && rest[i:i+6] == "</div>" {
			depth--
			if depth == 0 {
				endPos = i
				break
			}
		}
	}
	if endPos == 0 {
		if len(rest) > 2000 {
			rest = rest[:2000]
		}
		endPos = len(rest)
	}
	return strings.TrimSpace(rest[:endPos])
}

func scrapeWork(workID, siteCat string) (*Work, error) {
	siteCategories := []string{siteCat}
	if siteCat == "" {
		siteCategories = []string{"maniax", "home", "pro", "soft"}
	}

	var info *ajaxInfo
	var usedCat string
	for _, cat := range siteCategories {
		var err error
		info, err = fetchAjaxInfo(workID, cat)
		if err == nil {
			usedCat = cat
			break
		}
		if strings.Contains(err.Error(), "rate limited") {
			return nil, err
		}
	}

	if info == nil {
		return nil, fmt.Errorf("work %s not found in any category", workID)
	}

	w := &Work{
		ID:           workID,
		Title:        info.WorkName,
		CircleID:     info.MakerID,
		Rating:       info.RateAverage,
		RatingCount:  info.RateCount,
		ReleaseDate:  info.RegistDate,
		SiteCategory: usedCat,
		PageURL:      fmt.Sprintf("https://www.dlsite.com/%s/work/=/product_id/%s.html", usedCat, workID),
		FetchedAt:    time.Now().Format("2006-01-02 15:04:05"),
	}

	if info.WorkImage != "" {
		img := info.WorkImage
		if !strings.HasPrefix(img, "http") {
			img = "https:" + img
		}
		w.ImageURL = img
	}

	switch info.AgeCategory {
	case 1:
		w.AgeRating = "全年龄"
	case 2:
		w.AgeRating = "R-15"
	case 3:
		w.AgeRating = "R-18"
	}

	workTypeMap := map[string]string{
		"ACN": "动作", "ADV": "冒险", "RPG": "角色扮演", "SLN": "模拟",
		"STG": "射击", "PZL": "益智", "TBL": "桌游", "DNV": "电子小说",
		"SOU": "音声/ASMR", "MNG": "漫画", "ICG": "CG・插画",
		"MOV": "视频", "MUS": "音乐", "TOL": "工具", "ET3": "其他",
		"ETC": "其他游戏", "TYP": "打字", "QIZ": "问答", "NRE": "小说",
	}
	if name, ok := workTypeMap[info.WorkType]; ok {
		w.WorkType = name
	} else {
		w.WorkType = info.WorkType
	}

	if info.Price > 0 {
		w.Price = fmt.Sprintf("¥%d", info.Price)
	} else if info.PriceStr != "" {
		w.Price = "¥" + info.PriceStr
	}

	if info.DLCount > 0 {
		w.DLCount = fmt.Sprintf("%d", info.DLCount)
	}

	// Now fetch HTML for details
	htmlContent, err := fetchHTMLPage(workID, usedCat)
	if err == nil {
		desc := extractDescription(htmlContent)
		if desc != "" {
			w.Description = desc
		}

		circleRaw := parseTableField(htmlContent, "サークル名")
		if circleRaw == "" {
			circleRaw = parseTableField(htmlContent, "ブランド名")
		}
		if circleRaw != "" {
			w.Circle = extractText(circleRaw)
		}

		genresRaw := parseTableField(htmlContent, "ジャンル")
		if genresRaw != "" {
			w.Genres = extractLinks(genresRaw)
		}

		fileSize := parseTableField(htmlContent, "ファイル容量")
		if fileSize != "" {
			w.FileSize = extractText(fileSize)
		}

		updateDate := parseTableField(htmlContent, "更新情報")
		if updateDate != "" {
			w.UpdateDate = extractText(updateDate)
		}

		seriesRaw := parseTableField(htmlContent, "シリーズ名")
		if seriesRaw != "" {
			w.Series = extractText(seriesRaw)
		}

		fileFormatsRaw := parseTableField(htmlContent, "ファイル形式")
		if fileFormatsRaw != "" {
			w.FileFormats = extractLinks(fileFormatsRaw)
			if len(w.FileFormats) == 0 {
				t := extractText(fileFormatsRaw)
				if t != "" {
					w.FileFormats = []string{t}
				}
			}
		}

		w.SampleImages = extractSampleImages(htmlContent)

		// Try to get DL count from HTML if not from API
		if w.DLCount == "" {
			dlRe := regexp.MustCompile(`class="point"[^>]*>(\d[\d,]+)`)
			if m := dlRe.FindStringSubmatch(htmlContent); len(m) > 1 {
				w.DLCount = strings.ReplaceAll(m[1], ",", "")
			}
		}
	}

	return w, nil
}

func saveWork(w *Work) error {
	genres, _ := json.Marshal(w.Genres)
	fileFormats, _ := json.Marshal(w.FileFormats)
	samples, _ := json.Marshal(w.SampleImages)

	_, err := db.Exec(`INSERT OR REPLACE INTO works
		(id, title, circle, circle_id, description, image_url, sample_images,
		 price, rating, rating_count, dl_count, age_rating, work_type, file_size,
		 release_date, update_date, series, genres, file_formats, page_url,
		 status, category, fetched_at, site_category)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
				COALESCE((SELECT status FROM works WHERE id=?), 'normal'),
				COALESCE((SELECT category FROM works WHERE id=?), ''),
				?,?)`,
		w.ID, w.Title, w.Circle, w.CircleID, w.Description, w.ImageURL, string(samples),
		w.Price, w.Rating, w.RatingCount, w.DLCount, w.AgeRating, w.WorkType, w.FileSize,
		w.ReleaseDate, w.UpdateDate, w.Series, string(genres), string(fileFormats), w.PageURL,
		w.ID, w.ID, w.FetchedAt, w.SiteCategory)
	return err
}

func runScrape(ctx context.Context, task ScrapeTask) {
	scrapeMu.Lock()
	scrapeStatus = ScrapeStatus{
		Running:   true,
		StartedAt: time.Now().Format("2006-01-02 15:04:05"),
		Message:   "正在爬取...",
	}
	scrapeMu.Unlock()

	delay := time.Duration(task.DelayMs) * time.Millisecond
	if delay < 10*time.Second {
		delay = 10 * time.Second
	}

	prefix := task.Prefix
	if prefix == "" {
		prefix = "RJ"
	}
	siteCat := task.Category
	if siteCat == "" {
		siteCat = "maniax"
	}

	startTime := time.Now()
	consecutiveErrors := 0

	for id := task.StartID; id <= task.EndID; id++ {
		select {
		case <-ctx.Done():
			scrapeMu.Lock()
			scrapeStatus.Running = false
			scrapeStatus.Message = "已停止"
			scrapeMu.Unlock()
			return
		default:
		}

		workID := fmt.Sprintf("%s%06d", prefix, id)
		if id >= 1000000 {
			workID = fmt.Sprintf("%s%08d", prefix, id)
		}

		scrapeMu.Lock()
		scrapeStatus.CurrentID = workID
		elapsed := time.Since(startTime).Seconds()
		if elapsed > 0 && scrapeStatus.TotalDone > 0 {
			scrapeStatus.Speed = fmt.Sprintf("%.1f 个/分钟", float64(scrapeStatus.TotalDone)/elapsed*60)
		}
		scrapeMu.Unlock()

		work, err := scrapeWork(workID, siteCat)
		if err != nil {
			if strings.Contains(err.Error(), "rate limited") {
				log.Printf("被限速，等待60秒: %s", workID)
				scrapeMu.Lock()
				scrapeStatus.Message = "被限速，等待60秒..."
				scrapeMu.Unlock()
				select {
				case <-ctx.Done():
					scrapeMu.Lock()
					scrapeStatus.Running = false
					scrapeStatus.Message = "已停止"
					scrapeMu.Unlock()
					return
				case <-time.After(60 * time.Second):
				}
				consecutiveErrors++
				if consecutiveErrors > 5 {
					delay = delay * 2
					consecutiveErrors = 0
				}
				continue
			}
			scrapeMu.Lock()
			scrapeStatus.TotalErrors++
			scrapeStatus.TotalDone++
			scrapeMu.Unlock()
			consecutiveErrors++
			if consecutiveErrors > 20 {
				// Too many consecutive not-found, might be past the end
				log.Printf("连续%d个未找到，跳过一些", consecutiveErrors)
				consecutiveErrors = 0
			}
		} else {
			if err := saveWork(work); err != nil {
				log.Printf("保存失败 %s: %v", workID, err)
			}
			scrapeMu.Lock()
			scrapeStatus.TotalFound++
			scrapeStatus.TotalDone++
			scrapeStatus.Message = fmt.Sprintf("正在爬取... 最新: %s - %s", work.ID, work.Title)
			scrapeMu.Unlock()
			consecutiveErrors = 0
			log.Printf("已获取: %s - %s", work.ID, work.Title)
		}

		select {
		case <-ctx.Done():
			scrapeMu.Lock()
			scrapeStatus.Running = false
			scrapeStatus.Message = "已停止"
			scrapeMu.Unlock()
			return
		case <-time.After(delay):
		}
	}

	scrapeMu.Lock()
	scrapeStatus.Running = false
	scrapeStatus.Message = fmt.Sprintf("完成！共找到 %d 个作品", scrapeStatus.TotalFound)
	scrapeMu.Unlock()
}

// ── API Handlers ──

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func handleGetWorks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	perPage, _ := strconv.Atoi(q.Get("per_page"))
	if perPage < 1 || perPage > 100 {
		perPage = 30
	}

	where := []string{"1=1"}
	args := []interface{}{}

	if s := q.Get("status"); s != "" {
		if s == "blocked" {
			where = append(where, "status = 'blocked'")
		} else {
			where = append(where, "status != 'blocked'")
			if s != "all" {
				where = append(where, "status = ?")
				args = append(args, s)
			}
		}
	} else {
		where = append(where, "status != 'blocked'")
	}

	if cat := q.Get("category"); cat != "" {
		where = append(where, "category = ?")
		args = append(args, cat)
	}

	if search := q.Get("search"); search != "" {
		where = append(where, "(title LIKE ? OR circle LIKE ? OR id LIKE ? OR description LIKE ?)")
		like := "%" + search + "%"
		args = append(args, like, like, like, like)
	}

	if wt := q.Get("work_type"); wt != "" {
		where = append(where, "work_type = ?")
		args = append(args, wt)
	}

	if age := q.Get("age_rating"); age != "" {
		where = append(where, "age_rating = ?")
		args = append(args, age)
	}

	if minRating := q.Get("min_rating"); minRating != "" {
		r, _ := strconv.ParseFloat(minRating, 64)
		where = append(where, "rating >= ?")
		args = append(args, r)
	}

	whereClause := strings.Join(where, " AND ")

	// Sort
	sortField := "fetched_at"
	sortDir := "DESC"
	if s := q.Get("sort"); s != "" {
		allowed := map[string]string{
			"title": "title", "rating": "rating", "release_date": "release_date",
			"price": "price", "dl_count": "CAST(dl_count AS INTEGER)", "fetched_at": "fetched_at",
		}
		if f, ok := allowed[s]; ok {
			sortField = f
		}
	}
	if d := q.Get("dir"); d == "ASC" || d == "asc" {
		sortDir = "ASC"
	}

	// Count
	var total int
	countQuery := "SELECT COUNT(*) FROM works WHERE " + whereClause
	db.QueryRow(countQuery, args...).Scan(&total)

	offset := (page - 1) * perPage
	query := fmt.Sprintf("SELECT id, title, circle, circle_id, description, image_url, sample_images, price, rating, rating_count, dl_count, age_rating, work_type, file_size, release_date, update_date, series, genres, file_formats, page_url, status, category, fetched_at, site_category FROM works WHERE %s ORDER BY %s %s LIMIT ? OFFSET ?", whereClause, sortField, sortDir)
	args = append(args, perPage, offset)

	rows, err := db.Query(query, args...)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	works := []Work{}
	for rows.Next() {
		var wk Work
		var genresJSON, formatsJSON, samplesJSON string
		err := rows.Scan(&wk.ID, &wk.Title, &wk.Circle, &wk.CircleID, &wk.Description,
			&wk.ImageURL, &samplesJSON, &wk.Price, &wk.Rating, &wk.RatingCount,
			&wk.DLCount, &wk.AgeRating, &wk.WorkType, &wk.FileSize,
			&wk.ReleaseDate, &wk.UpdateDate, &wk.Series, &genresJSON,
			&formatsJSON, &wk.PageURL, &wk.Status, &wk.Category,
			&wk.FetchedAt, &wk.SiteCategory)
		if err != nil {
			continue
		}
		json.Unmarshal([]byte(genresJSON), &wk.Genres)
		json.Unmarshal([]byte(formatsJSON), &wk.FileFormats)
		json.Unmarshal([]byte(samplesJSON), &wk.SampleImages)
		if wk.Genres == nil {
			wk.Genres = []string{}
		}
		if wk.FileFormats == nil {
			wk.FileFormats = []string{}
		}
		if wk.SampleImages == nil {
			wk.SampleImages = []string{}
		}
		works = append(works, wk)
	}

	jsonResponse(w, map[string]interface{}{
		"works":    works,
		"total":    total,
		"page":     page,
		"per_page": perPage,
		"pages":    (total + perPage - 1) / perPage,
	})
}

func handleGetWork(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/works/")
	if id == "" {
		jsonError(w, "missing id", 400)
		return
	}

	var wk Work
	var genresJSON, formatsJSON, samplesJSON string
	err := db.QueryRow("SELECT id, title, circle, circle_id, description, image_url, sample_images, price, rating, rating_count, dl_count, age_rating, work_type, file_size, release_date, update_date, series, genres, file_formats, page_url, status, category, fetched_at, site_category FROM works WHERE id = ?", id).
		Scan(&wk.ID, &wk.Title, &wk.Circle, &wk.CircleID, &wk.Description,
			&wk.ImageURL, &samplesJSON, &wk.Price, &wk.Rating, &wk.RatingCount,
			&wk.DLCount, &wk.AgeRating, &wk.WorkType, &wk.FileSize,
			&wk.ReleaseDate, &wk.UpdateDate, &wk.Series, &genresJSON,
			&formatsJSON, &wk.PageURL, &wk.Status, &wk.Category,
			&wk.FetchedAt, &wk.SiteCategory)
	if err != nil {
		jsonError(w, "not found", 404)
		return
	}
	json.Unmarshal([]byte(genresJSON), &wk.Genres)
	json.Unmarshal([]byte(formatsJSON), &wk.FileFormats)
	json.Unmarshal([]byte(samplesJSON), &wk.SampleImages)
	if wk.Genres == nil {
		wk.Genres = []string{}
	}
	if wk.FileFormats == nil {
		wk.FileFormats = []string{}
	}
	if wk.SampleImages == nil {
		wk.SampleImages = []string{}
	}
	jsonResponse(w, wk)
}

func handleUpdateWork(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/works/")
	if id == "" {
		jsonError(w, "missing id", 400)
		return
	}

	var body struct {
		Status   *string `json:"status"`
		Category *string `json:"category"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", 400)
		return
	}

	if body.Status != nil {
		db.Exec("UPDATE works SET status = ? WHERE id = ?", *body.Status, id)
	}
	if body.Category != nil {
		db.Exec("UPDATE works SET category = ? WHERE id = ?", *body.Category, id)
	}
	jsonResponse(w, map[string]string{"ok": "true"})
}

func handleBatchUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs      []string `json:"ids"`
		Status   *string  `json:"status"`
		Category *string  `json:"category"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", 400)
		return
	}

	tx, _ := db.Begin()
	for _, id := range body.IDs {
		if body.Status != nil {
			tx.Exec("UPDATE works SET status = ? WHERE id = ?", *body.Status, id)
		}
		if body.Category != nil {
			tx.Exec("UPDATE works SET category = ? WHERE id = ?", *body.Category, id)
		}
	}
	tx.Commit()
	jsonResponse(w, map[string]string{"ok": "true", "count": fmt.Sprintf("%d", len(body.IDs))})
}

func handleFetchSingle(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ID       string `json:"id"`
		Category string `json:"site_category"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", 400)
		return
	}

	id := strings.ToUpper(strings.TrimSpace(body.ID))
	if !workIDRegex.MatchString(id) {
		jsonError(w, "无效的作品ID格式，示例: RJ123456", 400)
		return
	}

	cat := body.Category
	if cat == "" {
		cat = "maniax"
	}

	work, err := scrapeWork(id, cat)
	if err != nil {
		jsonError(w, fmt.Sprintf("获取失败: %v", err), 500)
		return
	}

	if err := saveWork(work); err != nil {
		jsonError(w, fmt.Sprintf("保存失败: %v", err), 500)
		return
	}

	jsonResponse(w, work)
}

func handleStartScrape(w http.ResponseWriter, r *http.Request) {
	scrapeMu.Lock()
	if scrapeStatus.Running {
		scrapeMu.Unlock()
		jsonError(w, "爬取任务已在运行中", 409)
		return
	}
	scrapeMu.Unlock()

	var task ScrapeTask
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		jsonError(w, "invalid body", 400)
		return
	}

	if task.StartID <= 0 || task.EndID <= 0 || task.StartID > task.EndID {
		jsonError(w, "无效的ID范围", 400)
		return
	}

	scrapeCtx, scrapeCancel = context.WithCancel(context.Background())
	go runScrape(scrapeCtx, task)

	jsonResponse(w, map[string]string{"ok": "true", "message": "爬取已启动"})
}

func handleStopScrape(w http.ResponseWriter, r *http.Request) {
	if scrapeCancel != nil {
		scrapeCancel()
	}
	jsonResponse(w, map[string]string{"ok": "true", "message": "正在停止"})
}

func handleScrapeStatus(w http.ResponseWriter, r *http.Request) {
	scrapeMu.Lock()
	status := scrapeStatus
	scrapeMu.Unlock()
	jsonResponse(w, status)
}

func handleGetCategories(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT name, color FROM categories ORDER BY created_at")
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	defer rows.Close()

	type Cat struct {
		Name  string `json:"name"`
		Color string `json:"color"`
		Count int    `json:"count"`
	}
	var cats []Cat
	for rows.Next() {
		var c Cat
		rows.Scan(&c.Name, &c.Color)
		db.QueryRow("SELECT COUNT(*) FROM works WHERE category = ?", c.Name).Scan(&c.Count)
		cats = append(cats, c)
	}
	if cats == nil {
		cats = []Cat{}
	}
	jsonResponse(w, cats)
}

func handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Color string `json:"color"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, "invalid body", 400)
		return
	}
	if body.Name == "" {
		jsonError(w, "name required", 400)
		return
	}
	if body.Color == "" {
		body.Color = "#6366f1"
	}
	_, err := db.Exec("INSERT OR REPLACE INTO categories(name, color) VALUES(?, ?)", body.Name, body.Color)
	if err != nil {
		jsonError(w, err.Error(), 500)
		return
	}
	jsonResponse(w, map[string]string{"ok": "true"})
}

func handleDeleteCategory(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		jsonError(w, "name required", 400)
		return
	}
	db.Exec("UPDATE works SET category = '' WHERE category = ?", name)
	db.Exec("DELETE FROM categories WHERE name = ?", name)
	jsonResponse(w, map[string]string{"ok": "true"})
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{}

	var total, blocked, categorized int
	db.QueryRow("SELECT COUNT(*) FROM works").Scan(&total)
	db.QueryRow("SELECT COUNT(*) FROM works WHERE status='blocked'").Scan(&blocked)
	db.QueryRow("SELECT COUNT(*) FROM works WHERE category != ''").Scan(&categorized)

	stats["total"] = total
	stats["blocked"] = blocked
	stats["categorized"] = categorized
	stats["visible"] = total - blocked

	// Work types
	rows, _ := db.Query("SELECT work_type, COUNT(*) as c FROM works WHERE status != 'blocked' GROUP BY work_type ORDER BY c DESC LIMIT 20")
	types := []map[string]interface{}{}
	if rows != nil {
		for rows.Next() {
			var t string
			var c int
			rows.Scan(&t, &c)
			if t != "" {
				types = append(types, map[string]interface{}{"name": t, "count": c})
			}
		}
		rows.Close()
	}
	stats["work_types"] = types

	jsonResponse(w, stats)
}

func handleImageProxy(w http.ResponseWriter, r *http.Request) {
	imgURL := r.URL.Query().Get("url")
	if imgURL == "" {
		http.Error(w, "missing url", 400)
		return
	}
	decoded, err := url.QueryUnescape(imgURL)
	if err == nil {
		imgURL = decoded
	}
	if !strings.HasPrefix(imgURL, "http") {
		imgURL = "https:" + imgURL
	}

	req, err := http.NewRequest("GET", imgURL, nil)
	if err != nil {
		http.Error(w, "bad url", 400)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
	req.Header.Set("Referer", "https://www.dlsite.com/")

	resp, err := httpClient.Do(req)
	if err != nil {
		http.Error(w, "fetch error", 502)
		return
	}
	defer resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.Header().Set("Cache-Control", "public, max-age=86400")
	io.Copy(w, resp.Body)
}

// ── Router ──

func apiRouter() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/works", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			handleGetWorks(w, r)
		default:
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/works/batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" || r.Method == "PATCH" {
			handleBatchUpdate(w, r)
		} else {
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/works/", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			handleGetWork(w, r)
		case "PUT", "PATCH":
			handleUpdateWork(w, r)
		default:
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/fetch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			handleFetchSingle(w, r)
		} else {
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/scrape/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			handleStartScrape(w, r)
		} else {
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/scrape/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			handleStopScrape(w, r)
		} else {
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/scrape/status", handleScrapeStatus)

	mux.HandleFunc("/api/categories", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			handleGetCategories(w, r)
		case "POST":
			handleCreateCategory(w, r)
		case "DELETE":
			handleDeleteCategory(w, r)
		default:
			jsonError(w, "method not allowed", 405)
		}
	})

	mux.HandleFunc("/api/stats", handleStats)

	mux.HandleFunc("/api/proxy/image", handleImageProxy)

	// Serve embedded web files
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/" {
			path = "/web/index.html"
		} else {
			path = "/web" + path
		}

		data, err := webFS.ReadFile(strings.TrimPrefix(path, "/"))
		if err != nil {
			// SPA fallback
			data, err = webFS.ReadFile("web/index.html")
			if err != nil {
				http.Error(w, "not found", 404)
				return
			}
		}

		// Set content type
		switch {
		case strings.HasSuffix(path, ".html"):
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		case strings.HasSuffix(path, ".css"):
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		case strings.HasSuffix(path, ".js"):
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		case strings.HasSuffix(path, ".svg"):
			w.Header().Set("Content-Type", "image/svg+xml")
		case strings.HasSuffix(path, ".png"):
			w.Header().Set("Content-Type", "image/png")
		case strings.HasSuffix(path, ".ico"):
			w.Header().Set("Content-Type", "image/x-icon")
		}
		w.Write(data)
	})

	return mux
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if err := initDB(); err != nil {
		log.Fatalf("数据库初始化失败: %v", err)
	}
	defer db.Close()
	log.Println("数据库已就绪")

	port := "8080"
	if p := os.Getenv("PORT"); p != "" {
		port = p
	}

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      apiRouter(),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
	}

	// Check if port is available
	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.Fatalf("端口 %s 被占用: %v", port, err)
	}
	ln.Close()

	go func() {
		log.Printf("DLsite 游戏信息浏览器已启动: http://localhost:%s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("服务器启动失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("正在关闭服务器...")
	if scrapeCancel != nil {
		scrapeCancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	log.Println("已关闭")
}
