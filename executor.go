// Package tsq provides type-safe SQL query helpers and code generation utilities.
package tsq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

var (
	errInsertRequiresColumn        = errors.New("insert requires at least one column")
	errInsertLayoutMismatch        = errors.New("batch insert requires matching column layouts")
	errUpdateRequiresMutableColumn = errors.New("update requires at least one mutable column")
	errUpdateRequiresPrimaryKey    = errors.New("update requires a non-zero primary key")
	errUpdateLayoutMismatch        = errors.New("batch update requires matching column layouts")
	errDeleteRequiresPrimaryKey    = errors.New("delete requires a non-zero primary key")
	errMutationItemNil             = errors.New("mutation item cannot be nil")
	errMutationItemTableMethod     = errors.New("mutation item must implement Table() string")
	errMutationItemPointer         = errors.New("mutation item must be a non-nil pointer")
	errMutationItemStructPointer   = errors.New("mutation item must point to a struct")
	errMutationItemNoTaggedFields  = errors.New("mutation item has no db-tagged fields")
)

// SqlExecutor defines the interface for executing SQL queries.
// It mirrors the gorp.SqlExecutor interface but is owned by tsq.
type SqlExecutor interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
	Exec(query string, args ...any) (sql.Result, error)
	WithContext(ctx context.Context) SqlExecutor
	SelectOne(dst any, query string, args ...any) error
	SelectInt(query string, args ...any) (int64, error)
	SelectNullInt(query string, args ...any) (sql.NullInt64, error)
	SelectFloat(query string, args ...any) (float64, error)
	SelectNullFloat(query string, args ...any) (sql.NullFloat64, error)
	SelectStr(query string, args ...any) (string, error)
	SelectNullStr(query string, args ...any) (sql.NullString, error)
	Select(dst any, query string, args ...any) (int, error)
	Insert(dst ...any) error
	Update(dst ...any) (int64, error)
	Delete(dst ...any) (int64, error)
}

// Dialect defines the interface for database dialect-specific operations.
// It mirrors the Dialect interface but is owned by tsq.
type Dialect interface {
	// QuoteField returns the dialect-specific quoted identifier
	QuoteField(field string) string
	// BindVar returns the dialect-specific bind variable placeholder
	BindVar(i int) string
	// CreateTableSuffix returns dialect-specific create table suffix
	CreateTableSuffix() string
	// CreateIndexSuffix returns dialect-specific create index suffix
	CreateIndexSuffix() string
	// DropIndexSuffix returns dialect-specific drop index suffix
	DropIndexSuffix() string
	// TruncateClause returns the dialect-specific truncate clause
	TruncateClause() string
	// AutoIncrementClause returns the dialect-specific auto-increment clause
	AutoIncrementClause() string
	// AutoIncrementBindValue returns the dialect-specific auto-increment bind value
	AutoIncrementBindValue() string
	// LastInsertIdReturningSuffix returns the dialect-specific returning suffix for last insert id
	LastInsertIdReturningSuffix(table, col string) string
	// AllTablesQuery returns the dialect-specific query to list all tables
	AllTablesQuery() string
	// CreateTableIfNotExistsSuffix returns the dialect-specific create if not exists suffix
	CreateTableIfNotExistsSuffix() string
	// HasConstraintsQuery returns the dialect-specific query to check constraints
	HasConstraintsQuery(string, string) string
}

// DbMap represents a database map that holds database connection and dialect information.
// It mirrors the DbMap structure but is owned by tsq.
type DbMap struct {
	Db      *sql.DB
	Dialect Dialect
	ctx     context.Context
}

// Query executes a query and returns rows.
func (db *DbMap) Query(query string, args ...any) (*sql.Rows, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}

	if db.ctx != nil {
		return db.Db.QueryContext(db.ctx, query, args...)
	}

	return db.Db.Query(query, args...)
}

// QueryRow executes a query that returns a single row.
func (db *DbMap) QueryRow(query string, args ...any) *sql.Row {
	if db == nil || db.Db == nil {
		return nil
	}

	if db.ctx != nil {
		return db.Db.QueryRowContext(db.ctx, query, args...)
	}

	return db.Db.QueryRow(query, args...)
}

// Exec executes a query without returning rows.
func (db *DbMap) Exec(query string, args ...any) (sql.Result, error) {
	if db == nil || db.Db == nil {
		return nil, nil
	}

	if db.ctx != nil {
		return db.Db.ExecContext(db.ctx, query, args...)
	}

	return db.Db.Exec(query, args...)
}

// WithContext returns a new DbMap with the specified context.
func (db *DbMap) WithContext(ctx context.Context) SqlExecutor {
	if db == nil {
		return nil
	}

	return &DbMap{
		Db:      db.Db,
		Dialect: db.Dialect,
		ctx:     ctx,
	}
}

// SelectOne executes a query and scans a single row into dst.
func (db *DbMap) SelectOne(dst any, query string, args ...any) error {
	rows, err := db.Query(query, args...)
	if err != nil {
		return err
	}

	defer func() {
		_ = rows.Close()
	}()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}

		return sql.ErrNoRows
	}

	// Use scanRow for proper struct tag handling
	return scanRow(rows, dst)
}

// scanRow handles scanning with struct tag support
func scanRow(rows *sql.Rows, dst any) error {
	if scanner, ok := dst.(sql.Scanner); ok {
		return rows.Scan(scanner)
	}

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Try direct Scan first (works for simple types)
	switch reflect.TypeOf(dst).Kind() {
	case reflect.Ptr:
		elem := reflect.ValueOf(dst).Elem()
		switch elem.Kind() {
		case reflect.Struct:
			// Use scanStruct for struct types with tag support
			return scanStruct(rows, cols, dst)
		default:
			// Simple types like *int, *string - use direct Scan
			return rows.Scan(dst)
		}
	}

	return rows.Scan(dst)
}

// scanStruct handles scanning into structs with db tag support
func scanStruct(rows *sql.Rows, cols []string, dst any) error {
	values := make([]any, len(cols))
	for i := range cols {
		values[i] = new(any)
	}

	if err := rows.Scan(values...); err != nil {
		return err
	}

	// Map columns to struct fields
	elem := reflect.ValueOf(dst).Elem()
	for i, col := range cols {
		val := reflect.ValueOf(values[i]).Elem().Elem()

		// Find field with matching db tag
		found := false

		for j := 0; j < elem.NumField(); j++ {
			field := elem.Type().Field(j)

			tag := field.Tag.Get("db")
			if tag == col || (tag == "" && strings.EqualFold(field.Name, col)) {
				if elem.Field(j).CanSet() && val.IsValid() {
					// Handle type conversion
					setField(elem.Field(j), val)
				}
				found = true

				break
			}
		}

		if !found && !isNilInterface(val) {
			// Try case-insensitive match
			for j := 0; j < elem.NumField(); j++ {
				field := elem.Type().Field(j)
				if strings.EqualFold(field.Name, col) {
					if elem.Field(j).CanSet() && val.IsValid() {
						setField(elem.Field(j), val)
					}

					break
				}
			}
		}
	}

	return nil
}

// setField sets a field value with type conversion
func setField(field, val reflect.Value) {
	if field.Type() == val.Type() {
		field.Set(val)
		return
	}

	// Handle type conversion
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch val.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetInt(val.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetInt(int64(val.Uint()))
		case reflect.Float32, reflect.Float64:
			field.SetInt(int64(val.Float()))
		}
	case reflect.String:
		if val.Kind() == reflect.String {
			field.SetString(val.String())
		}
	case reflect.Bool:
		if val.Kind() == reflect.Bool {
			field.SetBool(val.Bool())
		}
	case reflect.Float32, reflect.Float64:
		switch val.Kind() {
		case reflect.Float32, reflect.Float64:
			field.SetFloat(val.Float())
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			field.SetFloat(float64(val.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetFloat(float64(val.Uint()))
		}
	default:
		if field.CanSet() {
			field.Set(val)
		}
	}
}

func isNilInterface(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}

	if v.Kind() == reflect.Interface && v.IsNil() {
		return true
	}

	return false
}

// SelectInt executes a query and returns a single integer result.
func (db *DbMap) SelectInt(query string, args ...any) (int64, error) {
	var result sql.NullInt64

	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return 0, err
	}

	return result.Int64, nil
}

// SelectNullInt executes a query and returns a nullable integer result.
func (db *DbMap) SelectNullInt(query string, args ...any) (sql.NullInt64, error) {
	var result sql.NullInt64
	err := db.SelectOne(&result, query, args...)

	return result, err
}

// SelectFloat executes a query and returns a single float result.
func (db *DbMap) SelectFloat(query string, args ...any) (float64, error) {
	var result sql.NullFloat64

	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return 0, err
	}

	return result.Float64, nil
}

// SelectNullFloat executes a query and returns a nullable float result.
func (db *DbMap) SelectNullFloat(query string, args ...any) (sql.NullFloat64, error) {
	var result sql.NullFloat64
	err := db.SelectOne(&result, query, args...)

	return result, err
}

// SelectStr executes a query and returns a single string result.
func (db *DbMap) SelectStr(query string, args ...any) (string, error) {
	var result sql.NullString

	err := db.SelectOne(&result, query, args...)
	if err != nil {
		return "", err
	}

	return result.String, nil
}

// SelectNullStr executes a query and returns a nullable string result.
func (db *DbMap) SelectNullStr(query string, args ...any) (sql.NullString, error) {
	var result sql.NullString
	err := db.SelectOne(&result, query, args...)

	return result, err
}

// Select executes a query and scans multiple rows into dst.
func (db *DbMap) Select(dst any, query string, args ...any) (int, error) {
	rows, err := db.Query(query, args...)
	if err != nil {
		return 0, err
	}

	defer func() {
		_ = rows.Close()
	}()

	cols, err := rows.Columns()
	if err != nil {
		return 0, err
	}

	dstVal := reflect.ValueOf(dst)
	if dstVal.Kind() != reflect.Ptr {
		return 0, errors.New("dst must be a pointer")
	}

	sliceVal := dstVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return 0, errors.New("dst must be a pointer to a slice")
	}

	count := 0
	elemType := sliceVal.Type().Elem()

	for rows.Next() {
		// Create a new element
		elem := reflect.New(elemType)

		// Scan the row into the element
		if elemType.Kind() == reflect.Struct {
			if err := scanStruct(rows, cols, elem.Interface()); err != nil {
				return 0, err
			}
		} else {
			// Simple type
			if err := rows.Scan(elem.Interface()); err != nil {
				return 0, err
			}
		}

		// Append to the slice
		sliceVal.Set(reflect.Append(sliceVal, elem.Elem()))

		count++
	}

	return count, rows.Err()
}

// CreateTablesIfNotExists creates all tables if they don't exist.
func (db *DbMap) CreateTablesIfNotExists() error {
	// This is typically used during initialization
	// Implementation depends on registered tables
	return nil
}

type mutationField struct {
	name   string
	column string
	value  reflect.Value
}

type mutationRecord struct {
	tableName string
	fields    []mutationField
	pkField   mutationField
}

// Insert inserts objects into the database.
func (db *DbMap) Insert(dst ...any) error {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return err
	}

	for _, group := range groupInsertRecords(records) {
		if err := db.insertBatch(group); err != nil {
			return err
		}
	}

	return nil
}

// Update updates objects in the database.
func (db *DbMap) Update(dst ...any) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupUpdateRecords(records) {
		affected, err := db.updateBatch(group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

// Delete deletes objects from the database.
func (db *DbMap) Delete(dst ...any) (int64, error) {
	records, err := collectMutationRecords(dst)
	if err != nil {
		return 0, err
	}

	var total int64

	for _, group := range groupDeleteRecords(records) {
		affected, err := db.deleteBatch(group)
		if err != nil {
			return total, err
		}

		total += affected
	}

	return total, nil
}

func collectMutationRecords(dst []any) ([]mutationRecord, error) {
	records := make([]mutationRecord, 0, len(dst))

	for _, item := range dst {
		record, err := mutationMetadata(item)
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}

	return records, nil
}

func groupInsertRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		fields := insertFieldsForRecord(record)
		return record.tableName + "|" + record.pkField.column + "|" + strings.Join(mutationFieldColumns(fields), ",")
	})
}

func groupUpdateRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column + "|" +
			strings.Join(mutationFieldColumns(updateFieldsForRecord(record)), ",")
	})
}

func groupDeleteRecords(records []mutationRecord) [][]mutationRecord {
	return groupMutationRecords(records, func(record mutationRecord) string {
		return record.tableName + "|" + record.pkField.column
	})
}

func groupMutationRecords(records []mutationRecord, keyFn func(mutationRecord) string) [][]mutationRecord {
	if len(records) == 0 {
		return nil
	}

	groups := make([][]mutationRecord, 0)
	indexByKey := make(map[string]int)

	for _, record := range records {
		key := keyFn(record)
		if idx, ok := indexByKey[key]; ok {
			groups[idx] = append(groups[idx], record)
			continue
		}

		indexByKey[key] = len(groups)
		groups = append(groups, []mutationRecord{record})
	}

	return groups
}

func (db *DbMap) insertBatch(records []mutationRecord) error {
	if len(records) == 0 {
		return nil
	}

	insertFields := insertFieldsForRecord(records[0])
	if len(insertFields) == 0 {
		return errInsertRequiresColumn
	}

	for _, record := range records[1:] {
		if !mutationFieldColumnsEqual(insertFields, insertFieldsForRecord(record)) {
			return errInsertLayoutMismatch
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return err
	}

	quotedCols := make([]string, 0, len(insertFields))

	for _, field := range insertFields {
		col, err := db.quoteMutationIdentifier(field.column)
		if err != nil {
			return err
		}

		quotedCols = append(quotedCols, col)
	}

	var (
		argIndex     int
		args         = make([]any, 0, len(insertFields)*len(records))
		valueClauses = make([]string, 0, len(records))
	)

	for _, record := range records {
		recordFields := insertFieldsForRecord(record)
		placeholders := make([]string, 0, len(recordFields))

		for _, field := range recordFields {
			placeholders = append(placeholders, db.nextBindVar(&argIndex))
			args = append(args, field.value.Interface())
		}

		valueClauses = append(valueClauses, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		tableSQL,
		strings.Join(quotedCols, ", "),
		strings.Join(valueClauses, ", "),
	)

	result, err := db.Exec(query, args...)
	if err != nil {
		return err
	}

	assignBatchInsertIDs(db, records, result, len(insertFields) != len(records[0].fields))

	return nil
}

func (db *DbMap) updateBatch(records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	updateFields := updateFieldsForRecord(records[0])
	if len(updateFields) == 0 {
		return 0, errUpdateRequiresMutableColumn
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errUpdateRequiresPrimaryKey
		}

		if !mutationFieldColumnsEqual(updateFields, updateFieldsForRecord(record)) {
			return 0, errUpdateLayoutMismatch
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return 0, err
	}

	pkSQL, err := db.quoteMutationIdentifier(records[0].pkField.column)
	if err != nil {
		return 0, err
	}

	var (
		argIndex   int
		args       []any
		setClauses = make([]string, 0, len(updateFields))
	)

	for _, field := range updateFields {
		colSQL, err := db.quoteMutationIdentifier(field.column)
		if err != nil {
			return 0, err
		}

		var clause strings.Builder
		clause.WriteString(colSQL)
		clause.WriteString(" = CASE ")
		clause.WriteString(pkSQL)

		for _, record := range records {
			recordField := mutationFieldByColumn(record.fields, field.column)

			clause.WriteString(" WHEN ")
			clause.WriteString(db.nextBindVar(&argIndex))
			clause.WriteString(" THEN ")
			clause.WriteString(db.nextBindVar(&argIndex))

			args = append(args, record.pkField.value.Interface(), recordField.value.Interface())
		}

		clause.WriteString(" ELSE ")
		clause.WriteString(colSQL)
		clause.WriteString(" END")
		setClauses = append(setClauses, clause.String())
	}

	wherePlaceholders := make([]string, 0, len(records))
	for _, record := range records {
		wherePlaceholders = append(wherePlaceholders, db.nextBindVar(&argIndex))
		args = append(args, record.pkField.value.Interface())
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s IN (%s)",
		tableSQL,
		strings.Join(setClauses, ", "),
		pkSQL,
		strings.Join(wherePlaceholders, ", "),
	)

	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func (db *DbMap) deleteBatch(records []mutationRecord) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	for _, record := range records {
		if isZeroMutationValue(record.pkField.value) {
			return 0, errDeleteRequiresPrimaryKey
		}
	}

	tableSQL, err := db.quoteMutationIdentifier(records[0].tableName)
	if err != nil {
		return 0, err
	}

	pkSQL, err := db.quoteMutationIdentifier(records[0].pkField.column)
	if err != nil {
		return 0, err
	}

	var (
		argIndex     int
		args         = make([]any, 0, len(records))
		placeholders = make([]string, 0, len(records))
	)

	for _, record := range records {
		placeholders = append(placeholders, db.nextBindVar(&argIndex))
		args = append(args, record.pkField.value.Interface())
	}

	query := fmt.Sprintf(
		"DELETE FROM %s WHERE %s IN (%s)",
		tableSQL,
		pkSQL,
		strings.Join(placeholders, ", "),
	)

	result, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

func mutationMetadata(dst any) (mutationRecord, error) {
	if dst == nil {
		return mutationRecord{}, errMutationItemNil
	}

	tabler, ok := dst.(interface{ Table() string })
	if !ok {
		return mutationRecord{}, errMutationItemTableMethod
	}

	value := reflect.ValueOf(dst)
	if value.Kind() != reflect.Ptr || value.IsNil() {
		return mutationRecord{}, errMutationItemPointer
	}

	value = value.Elem()
	if value.Kind() != reflect.Struct {
		return mutationRecord{}, errMutationItemStructPointer
	}

	fields := collectMutationFields(value)
	if len(fields) == 0 {
		return mutationRecord{}, errMutationItemNoTaggedFields
	}

	pkField, err := primaryMutationField(fields)
	if err != nil {
		return mutationRecord{}, err
	}

	return mutationRecord{
		tableName: tabler.Table(),
		fields:    fields,
		pkField:   pkField,
	}, nil
}

func collectMutationFields(value reflect.Value) []mutationField {
	fields := make([]mutationField, 0, value.NumField())
	valueType := value.Type()

	for i := 0; i < value.NumField(); i++ {
		fieldValue := value.Field(i)

		fieldType := valueType.Field(i)
		if fieldType.Anonymous && fieldValue.Kind() == reflect.Struct {
			fields = append(fields, collectMutationFields(fieldValue)...)
			continue
		}

		if !fieldValue.CanSet() {
			continue
		}

		column := parseDBColumn(fieldType.Tag.Get("db"))
		if column == "" {
			continue
		}

		fields = append(fields, mutationField{
			name:   fieldType.Name,
			column: column,
			value:  fieldValue,
		})
	}

	return fields
}

func insertFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields))
	for _, field := range record.fields {
		if field.column == record.pkField.column && isZeroMutationValue(field.value) {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func updateFieldsForRecord(record mutationRecord) []mutationField {
	fields := make([]mutationField, 0, len(record.fields)-1)
	for _, field := range record.fields {
		if field.column == record.pkField.column {
			continue
		}

		fields = append(fields, field)
	}

	return fields
}

func mutationFieldColumns(fields []mutationField) []string {
	cols := make([]string, 0, len(fields))
	for _, field := range fields {
		cols = append(cols, field.column)
	}

	return cols
}

func mutationFieldColumnsEqual(expected, actual []mutationField) bool {
	if len(expected) != len(actual) {
		return false
	}

	for i := range expected {
		if expected[i].column != actual[i].column {
			return false
		}
	}

	return true
}

func mutationFieldByColumn(fields []mutationField, column string) mutationField {
	for _, field := range fields {
		if field.column == column {
			return field
		}
	}

	return mutationField{}
}

func primaryMutationField(fields []mutationField) (mutationField, error) {
	for _, field := range fields {
		if field.column == "id" || field.column == "uid" || field.name == "ID" || field.name == "UID" {
			return field, nil
		}
	}

	return mutationField{}, errors.New("mutation item must contain an ID or UID field")
}

func parseDBColumn(tag string) string {
	if tag == "" || tag == "-" {
		return ""
	}

	parts := strings.Split(tag, ",")

	return strings.TrimSpace(parts[0])
}

func (db *DbMap) quoteMutationIdentifier(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("identifier cannot be empty")
	}

	if !builtInIdentifierPattern.MatchString(name) {
		return "", fmt.Errorf("invalid identifier: %s", name)
	}

	if db != nil && db.Dialect != nil {
		return db.Dialect.QuoteField(name), nil
	}

	return name, nil
}

func (db *DbMap) bindVar(index int) string {
	if db != nil && db.Dialect != nil {
		return db.Dialect.BindVar(index)
	}

	return "?"
}

func isZeroMutationValue(value reflect.Value) bool {
	return value.IsZero()
}

func (db *DbMap) nextBindVar(index *int) string {
	placeholder := db.bindVar(*index)
	(*index)++

	return placeholder
}

func assignMutationID(field reflect.Value, id int64) {
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		field.SetInt(id)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		field.SetUint(uint64(id))
	}
}

func assignBatchInsertIDs(db *DbMap, records []mutationRecord, result sql.Result, omittedPrimaryKey bool) {
	if !omittedPrimaryKey || len(records) == 0 {
		return
	}

	lastID, err := result.LastInsertId()
	if err != nil {
		return
	}

	if len(records) == 1 {
		assignMutationID(records[0].pkField.value, lastID)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil || rowsAffected != int64(len(records)) {
		return
	}

	var startID int64

	switch db.Dialect.(type) {
	case SqliteDialect, *SqliteDialect:
		startID = lastID - rowsAffected + 1
	case MySQLDialect, *MySQLDialect:
		startID = lastID
	default:
		return
	}

	for i, record := range records {
		assignMutationID(record.pkField.value, startID+int64(i))
	}
}

// AddTableWithName registers a table mapping (stub implementation for gorp compatibility)
func (db *DbMap) AddTableWithName(dst any, name string) *DbMapTable {
	// This is a stub implementation kept for gorp compatibility
	return &DbMapTable{}
}

// DbMapTable is a stub for method chaining compatibility
type DbMapTable struct{}

// SetKeys sets the primary keys (stub for gorp compatibility)
func (t *DbMapTable) SetKeys(autoincr bool, keynames ...string) *DbMapTable {
	return t
}

// SetVersionCol sets the version column (stub for gorp compatibility)
func (t *DbMapTable) SetVersionCol(name string) *DbMapTable {
	return t
}

// SqliteDialect is the SQLite dialect implementation.
type SqliteDialect struct{}

func (d SqliteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d SqliteDialect) BindVar(i int) string {
	return "?"
}

func (d SqliteDialect) CreateTableSuffix() string {
	return ";"
}

func (d SqliteDialect) CreateIndexSuffix() string {
	return ";"
}

func (d SqliteDialect) DropIndexSuffix() string {
	return ";"
}

func (d SqliteDialect) TruncateClause() string {
	return "DELETE FROM"
}

func (d SqliteDialect) AutoIncrementClause() string {
	return "AUTOINCREMENT"
}

func (d SqliteDialect) AutoIncrementBindValue() string {
	return ""
}

func (d SqliteDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d SqliteDialect) AllTablesQuery() string {
	return "SELECT name FROM sqlite_master WHERE type='table'"
}

func (d SqliteDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d SqliteDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// MySQLDialect is the MySQL dialect implementation.
type MySQLDialect struct{}

func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

func (d MySQLDialect) BindVar(i int) string {
	return "?"
}

func (d MySQLDialect) CreateTableSuffix() string {
	return ";"
}

func (d MySQLDialect) CreateIndexSuffix() string {
	return ";"
}

func (d MySQLDialect) DropIndexSuffix() string {
	return ";"
}

func (d MySQLDialect) TruncateClause() string {
	return "TRUNCATE"
}

func (d MySQLDialect) AutoIncrementClause() string {
	return "AUTO_INCREMENT"
}

func (d MySQLDialect) AutoIncrementBindValue() string {
	return ""
}

func (d MySQLDialect) LastInsertIdReturningSuffix(table, col string) string {
	return ""
}

func (d MySQLDialect) AllTablesQuery() string {
	return "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()"
}

func (d MySQLDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d MySQLDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// PostgresDialect is the PostgreSQL dialect implementation.
type PostgresDialect struct{}

func (d PostgresDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

func (d PostgresDialect) BindVar(i int) string {
	// PostgreSQL uses $1, $2, etc for bind variables (1-indexed)
	return "$" + strconv.Itoa(i+1)
}

func (d PostgresDialect) CreateTableSuffix() string {
	return ";"
}

func (d PostgresDialect) CreateIndexSuffix() string {
	return ";"
}

func (d PostgresDialect) DropIndexSuffix() string {
	return ";"
}

func (d PostgresDialect) TruncateClause() string {
	return "TRUNCATE"
}

func (d PostgresDialect) AutoIncrementClause() string {
	return ""
}

func (d PostgresDialect) AutoIncrementBindValue() string {
	return ""
}

func (d PostgresDialect) LastInsertIdReturningSuffix(table, col string) string {
	return " RETURNING " + d.QuoteField(col)
}

func (d PostgresDialect) AllTablesQuery() string {
	return "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"
}

func (d PostgresDialect) CreateTableIfNotExistsSuffix() string {
	return "IF NOT EXISTS"
}

func (d PostgresDialect) HasConstraintsQuery(table, column string) string {
	return ""
}

// wrappedExecutor wraps a standard SQL executor with dialect information.
type wrappedExecutor struct {
	SqlExecutor
	dialect Dialect
}

func (w wrappedExecutor) TSQDialect() Dialect {
	return w.dialect
}

// WrapExecutor wraps a SqlExecutor with dialect information.
func WrapExecutor(exec SqlExecutor, dialect Dialect) SqlExecutor {
	if exec == nil {
		return nil
	}

	if _, ok := exec.(dialectProvider); ok {
		return exec
	}

	return wrappedExecutor{
		SqlExecutor: exec,
		dialect:     dialect,
	}
}

// WrapDBMapExecutor wraps a SqlExecutor with dialect from DbMap.
func WrapDBMapExecutor(exec SqlExecutor, db *DbMap) SqlExecutor {
	if db == nil {
		return exec
	}

	return WrapExecutor(exec, db.Dialect)
}
