package tsq

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

func (spec querySpec[O]) buildCTEPrefix(useKeyword bool) (string, []any, error) {
	defs, err := spec.collectCTEDefinitions(useKeyword)
	if err != nil {
		return "", nil, err
	}

	if len(defs) == 0 {
		return "", nil, nil
	}

	parts := make([]string, 0, len(defs))
	args := make([]any, 0)

	for _, def := range defs {
		bodySQL, bodyArgs := def.buildBody(false)
		parts = append(parts, rawIdentifier(def.name)+" AS ("+bodySQL+")")
		args = append(args, bodyArgs...)
	}

	return "WITH " + strings.Join(parts, ", ") + " ", args, nil
}

func (spec querySpec[O]) collectCTEDefinitions(useKeyword bool) ([]cteDefinition, error) {
	collector := &cteCollector{
		seen:     make(map[string]struct{}),
		visiting: make(map[string]struct{}),
	}

	if err := collectCTEFromSpec(collector, spec, useKeyword); err != nil {
		return nil, err
	}

	return collector.ordered, nil
}

type cteCollector struct {
	ordered  []cteDefinition
	seen     map[string]struct{}
	visiting map[string]struct{}
}

func collectCTEFromSpec[O Owner](c *cteCollector, spec querySpec[O], useKeyword bool) error {
	var tables map[string]Table
	if useKeyword {
		tables = spec.pageQueryTables()
	} else {
		tables = spec.listQueryTables()
	}

	tableNames := make([]string, 0, len(tables))
	for name := range tables {
		tableNames = append(tableNames, name)
	}

	sort.Strings(tableNames)

	for _, name := range tableNames {
		provider, ok := tables[name].(cteProvider)
		if !ok {
			continue
		}

		if err := c.collectDefinition(provider.cteDefinition()); err != nil {
			return err
		}
	}

	for _, op := range spec.SetOps {
		if err := collectCTEFromSpec(c, op.spec, useKeyword); err != nil {
			return err
		}
	}

	return nil
}

func (c *cteCollector) collectDefinition(def cteDefinition) error {
	if strings.TrimSpace(def.name) == "" {
		return errors.New("cte name cannot be empty")
	}

	if _, exists := c.seen[def.name]; exists {
		return nil
	}

	if _, visiting := c.visiting[def.name]; visiting {
		return fmt.Errorf("cyclic CTE dependency detected for %s", def.name)
	}

	if def.selectCount == 0 {
		return fmt.Errorf("cte %s requires at least one selected column", def.name)
	}

	if def.keywordCount > 0 {
		return fmt.Errorf("cte %s does not support keyword search", def.name)
	}

	if err := def.validate(); err != nil {
		return err
	}

	c.visiting[def.name] = struct{}{}
	if err := def.collectNested(c, false); err != nil {
		delete(c.visiting, def.name)
		return err
	}

	delete(c.visiting, def.name)

	c.seen[def.name] = struct{}{}
	c.ordered = append(c.ordered, def)

	return nil
}
