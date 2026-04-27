package tsq

import (
	"sync"
)

// KeywordRegistry is a registry for dialect-specific SQL keywords
type KeywordRegistry struct {
	mu            sync.RWMutex
	keywords      map[DialectName]map[string]bool
	capabilities  map[DialectName]map[string]bool
}

var (
	// Global keyword registry
	globalRegistry *KeywordRegistry
	registryOnce   sync.Once
)

// GetKeywordRegistry returns the global keyword registry
func GetKeywordRegistry() *KeywordRegistry {
	registryOnce.Do(func() {
		globalRegistry = NewKeywordRegistry()
		globalRegistry.initializeDefaults()
	})
	return globalRegistry
}

// NewKeywordRegistry creates a new keyword registry
func NewKeywordRegistry() *KeywordRegistry {
	return &KeywordRegistry{
		keywords:     make(map[DialectName]map[string]bool),
		capabilities: make(map[DialectName]map[string]bool),
	}
}

// initializeDefaults sets up default keywords for known dialects
func (r *KeywordRegistry) initializeDefaults() {
	// MySQL keywords
	r.registerKeywords(DialectMySQL, []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER",
		"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION",
	})
	r.registerCapability(DialectMySQL, "FULL_OUTER_JOIN", false)
	r.registerCapability(DialectMySQL, "CTE", false)

	// PostgreSQL keywords
	r.registerKeywords(DialectPostgres, []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "FULL",
		"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION",
		"WITH", // CTE support
	})
	r.registerCapability(DialectPostgres, "FULL_OUTER_JOIN", true)
	r.registerCapability(DialectPostgres, "CTE", true)

	// SQLite keywords
	r.registerKeywords(DialectSQLite, []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS",
		"GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET",
	})
	r.registerCapability(DialectSQLite, "FULL_OUTER_JOIN", false)
	r.registerCapability(DialectSQLite, "CTE", true)

	// Oracle keywords
	r.registerKeywords(DialectOracle, []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "FULL",
		"GROUP", "BY", "ORDER", "HAVING", "ROWNUM",
	})
	r.registerCapability(DialectOracle, "FULL_OUTER_JOIN", true)
	r.registerCapability(DialectOracle, "CTE", true)

	// SQL Server keywords
	r.registerKeywords(DialectSQLServer, []string{
		"SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "CROSS",
		"GROUP", "BY", "ORDER", "HAVING", "OFFSET", "ROWS", "FETCH",
	})
	r.registerCapability(DialectSQLServer, "FULL_OUTER_JOIN", false)
	r.registerCapability(DialectSQLServer, "CTE", true)
}

// registerKeywords registers keywords for a dialect
func (r *KeywordRegistry) registerKeywords(dialect DialectName, keywords []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.keywords[dialect]; !ok {
		r.keywords[dialect] = make(map[string]bool)
	}

	for _, kw := range keywords {
		r.keywords[dialect][kw] = true
	}
}

// registerCapability registers a capability for a dialect
func (r *KeywordRegistry) registerCapability(dialect DialectName, capability string, supported bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.capabilities[dialect]; !ok {
		r.capabilities[dialect] = make(map[string]bool)
	}

	r.capabilities[dialect][capability] = supported
}

// RegisterDialect allows registering custom dialects
func (r *KeywordRegistry) RegisterDialect(dialect DialectName) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.keywords[dialect]; !ok {
		r.keywords[dialect] = make(map[string]bool)
	}
	if _, ok := r.capabilities[dialect]; !ok {
		r.capabilities[dialect] = make(map[string]bool)
	}
}

// AddKeyword adds a keyword for a dialect
func (r *KeywordRegistry) AddKeyword(dialect DialectName, keyword string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.keywords[dialect]; !ok {
		r.keywords[dialect] = make(map[string]bool)
	}
	r.keywords[dialect][keyword] = true
}

// IsKeyword checks if a string is a keyword for a dialect
func (r *KeywordRegistry) IsKeyword(dialect DialectName, keyword string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if kw, ok := r.keywords[dialect]; ok {
		return kw[keyword]
	}
	return false
}

// SetCapability sets a capability for a dialect
func (r *KeywordRegistry) SetCapability(dialect DialectName, capability string, supported bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.capabilities[dialect]; !ok {
		r.capabilities[dialect] = make(map[string]bool)
	}
	r.capabilities[dialect][capability] = supported
}

// HasCapability checks if a dialect supports a capability
func (r *KeywordRegistry) HasCapability(dialect DialectName, capability string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if cap, ok := r.capabilities[dialect]; ok {
		return cap[capability]
	}
	return false
}

// GetCapabilities returns all capabilities for a dialect
func (r *KeywordRegistry) GetCapabilities(dialect DialectName) map[string]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if cap, ok := r.capabilities[dialect]; ok {
		result := make(map[string]bool)
		for k, v := range cap {
			result[k] = v
		}
		return result
	}
	return make(map[string]bool)
}

// DialectValidator validates operations for a specific dialect
type DialectValidator struct {
	dialect Dialect
	registry *KeywordRegistry
}

// NewDialectValidator creates a validator for a dialect
func NewDialectValidator(dialect Dialect) *DialectValidator {
	return &DialectValidator{
		dialect: dialect,
		registry: GetKeywordRegistry(),
	}
}

// ValidateCapability checks if a dialect supports an operation
func (v *DialectValidator) ValidateCapability(operation string) error {
	if v.dialect == nil {
		return nil
	}

	dialName := detectDialectName(v.dialect)
	if !v.registry.HasCapability(dialName, operation) {
		return NewErrUnsupportedOperation(
			operation,
			dialName,
			"This operation is not supported by "+string(dialName),
		)
	}

	return nil
}

// DialectExtension allows extending dialect support
type DialectExtension struct {
	Name        DialectName
	Keywords    []string
	Capabilities map[string]bool
}

// ExtendDialect adds or updates dialect support
func ExtendDialect(ext DialectExtension) error {
	registry := GetKeywordRegistry()
	
	// Register dialect
	registry.RegisterDialect(ext.Name)
	
	// Add keywords
	registry.registerKeywords(ext.Name, ext.Keywords)
	
	// Add capabilities
	for cap, supported := range ext.Capabilities {
		registry.SetCapability(ext.Name, cap, supported)
	}

	return nil
}
