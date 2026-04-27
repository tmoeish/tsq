package tsq

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sync"
)

// SQLCacheConfig configures the SQL rendering cache
type SQLCacheConfig struct {
	// Enabled turns on SQL query caching
	Enabled bool
	// MaxSize is the maximum number of queries to cache (default: 1000)
	MaxSize int
}

// SQLRenderCache is an optional LRU cache for compiled SQL queries
// It caches rendered SQL strings keyed by a hash of the query specification
type SQLRenderCache struct {
	mu    sync.RWMutex
	cache map[string]cachedSQL
	order []string // Simple LRU tracking
	config SQLCacheConfig
	
	hitCount  int64
	missCount int64
}

type cachedSQL struct {
	canonicalSQL string
	timestamp    int64
}

// NewSQLRenderCache creates a new SQL rendering cache with configuration
func NewSQLRenderCache(config SQLCacheConfig) *SQLRenderCache {
	if !config.Enabled {
		return nil
	}

	if config.MaxSize <= 0 {
		config.MaxSize = 1000
	}

	return &SQLRenderCache{
		cache:  make(map[string]cachedSQL),
		order:  make([]string, 0, config.MaxSize),
		config: config,
	}
}

// cacheKeyForSpec generates a cache key from query spec
func (c *SQLRenderCache) cacheKeyForSpec(spec QuerySpec) string {
	// Create a hash of the spec for cache key
	h := md5.New()
	
	// Include all relevant parts of the spec
	fmt.Fprintf(h, "selects:%d;", len(spec.Selects))
	fmt.Fprintf(h, "filters:%d;", len(spec.Filters))
	fmt.Fprintf(h, "joins:%d;", len(spec.Joins))
	fmt.Fprintf(h, "groupby:%d;", len(spec.GroupBy))
	fmt.Fprintf(h, "having:%d;", len(spec.Having))
	fmt.Fprintf(h, "keyword_search:%d;", len(spec.KeywordSearch))
	
	return hex.EncodeToString(h.Sum(nil))
}

// Get retrieves a cached SQL string, returns (sql, found, hitCount, missCount)
func (c *SQLRenderCache) Get(spec QuerySpec) (string, bool) {
	if c == nil {
		return "", false
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	key := c.cacheKeyForSpec(spec)
	if cached, ok := c.cache[key]; ok {
		c.hitCount++
		return cached.canonicalSQL, true
	}

	c.missCount++
	return "", false
}

// Put stores a rendered SQL string in the cache
func (c *SQLRenderCache) Put(spec QuerySpec, canonicalSQL string) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.cacheKeyForSpec(spec)
	
	// Check if already cached
	if _, exists := c.cache[key]; exists {
		return
	}

	// If at capacity, evict oldest
	if len(c.cache) >= c.config.MaxSize && c.config.MaxSize > 0 {
		// Simple FIFO eviction when at max size
		if len(c.order) > 0 {
			oldest := c.order[0]
			c.order = c.order[1:]
			delete(c.cache, oldest)
		}
	}

	c.cache[key] = cachedSQL{
		canonicalSQL: canonicalSQL,
		timestamp:    0, // Could track time if needed
	}
	c.order = append(c.order, key)
}

// Stats returns cache statistics
func (c *SQLRenderCache) Stats() map[string]interface{} {
	if c == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	total := c.hitCount + c.missCount
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(c.hitCount) / float64(total) * 100
	}

	return map[string]interface{}{
		"enabled":   c.config.Enabled,
		"max_size":  c.config.MaxSize,
		"size":      len(c.cache),
		"hits":      c.hitCount,
		"misses":    c.missCount,
		"hit_rate":  hitRate,
	}
}

// Clear removes all cached entries
func (c *SQLRenderCache) Clear() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]cachedSQL)
	c.order = make([]string, 0, c.config.MaxSize)
}

// Reset clears cache and stats
func (c *SQLRenderCache) Reset() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]cachedSQL)
	c.order = make([]string, 0, c.config.MaxSize)
	c.hitCount = 0
	c.missCount = 0
}
