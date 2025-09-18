# pg_get_indexdef_worker

## Location
src/backend/utils/adt/ruleutils.c: 1250 - 1567

## Overview
The internal workhorse function that decompiles PostgreSQL index definitions into readable SQL statements, supporting both regular indexes and exclusion constraints with comprehensive customization options.

## Definition
```c
static char *pg_get_indexdef_worker(Oid indexrelid, int colno,
                                   const Oid *excludeOps,
                                   bool attrsOnly, bool keysOnly,
                                   bool showTblSpc, bool inherits,
                                   int prettyFlags, bool missing_ok)
```

## Detailed Description
This comprehensive function serves as the core implementation for generating human-readable index definitions from PostgreSQL's internal catalog information. It reconstructs the complete CREATE INDEX statement or portions thereof by examining pg_index, pg_class, and pg_am system catalogs.

The function handles both regular B-tree indexes and exclusion constraints, supporting advanced features like partial indexes, expression indexes, included columns, collation specifications, operator classes, tablespace assignments, and various index options. It provides fine-grained control over output formatting and content through multiple boolean parameters.

The implementation follows a systematic approach: it first retrieves catalog information, then processes index attributes (both key and included columns), handles special cases like expressions and constraints, and finally assembles the complete definition with appropriate SQL syntax.

## Parameters / Member Variables
- `indexrelid`: OID of the index relation to decompile
- `colno`: Specific column number to focus on (0 for all columns)
- `excludeOps`: Array of exclusion operator OIDs for exclusion constraints (NULL for regular indexes)
- `attrsOnly`: If true, return only attribute definitions without CREATE INDEX wrapper
- `keysOnly`: If true, exclude non-key (INCLUDE) columns from output
- `showTblSpc`: If true, include TABLESPACE clause in output
- `inherits`: Controls whether to include ONLY keyword for partitioned indexes
- `prettyFlags`: Formatting flags controlling pretty-printing behavior
- `missing_ok`: If true, return NULL instead of error for non-existent indexes

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1, SysCacheGetAttrNotNull (system catalog access)
  - GetIndexAmRoutine (access method information)
  - deparse_expression_pretty (expression formatting)
  - get_attname, get_atttypetypmodcoll (attribute information)
  - generate_relation_name, generate_qualified_relation_name (relation naming)
  - quote_identifier (identifier quoting)
  - get_opclass_name, generate_collation_name (index option formatting)
  - flatten_reloptions, get_reloptions (option handling)
- Called from (representative examples):
  - pg_get_indexdef (public interface for complete index definitions)
  - pg_get_indexdef_columns (key columns only)
  - pg_get_indexdef_columns_extended (configurable column definitions)
  - pg_get_constraintdef_worker (exclusion constraint definitions)

## Notes and Other Information
- This is a static function serving as the implementation foundation for all public index definition functions
- Supports both regular indexes and exclusion constraints through the excludeOps parameter
- Handles complex index features: expression indexes, partial indexes, included columns, custom collations, operator classes, and index options
- The function performs extensive system catalog lookups and requires appropriate locking
- Returns a palloc'd string that must be freed by the caller
- Error handling includes both immediate errors and graceful failure via missing_ok parameter
- The prettyFlags parameter controls SQL formatting for readability
- Supports both complete CREATE INDEX statements and partial definitions for specialized use cases