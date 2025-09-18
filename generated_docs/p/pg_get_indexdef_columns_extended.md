# pg_get_indexdef_columns_extended

## Location
src/backend/utils/adt/ruleutils.c: 1229 - 1249

## Overview
Provides an extensible internal interface for retrieving PostgreSQL index column definitions with configurable behavior controlled through bit flags.

## Definition
```c
char *pg_get_indexdef_columns_extended(Oid indexrelid, bits16 flags)
```

## Detailed Description
This function extends the basic pg_get_indexdef_columns functionality by accepting a flags parameter that allows fine-grained control over the output format and content. It serves as a more flexible internal interface that can be configured to produce different types of index column definitions based on the provided flags.

The function extracts specific flag values to determine formatting (RULE_INDEXDEF_PRETTY) and content scope (RULE_INDEXDEF_KEYS_ONLY), then delegates the actual work to pg_get_indexdef_worker with appropriate parameters. This design provides a clean abstraction for callers who need specific control over the index definition output without having to understand all the internal parameters of the worker function.

## Parameters / Member Variables
- `indexrelid`: The OID of the index relation for which to retrieve column definitions
- `flags`: A bits16 value containing flag bits that control output formatting and content (supports RULE_INDEXDEF_PRETTY and RULE_INDEXDEF_KEYS_ONLY flags)

## Dependencies
- Functions called/Symbols referenced:
  - RULE_INDEXDEF_PRETTY (flag constant for pretty formatting)
  - RULE_INDEXDEF_KEYS_ONLY (flag constant for keys-only output)
  - GET_PRETTY_FLAGS (macro for converting boolean to formatting flags)
  - pg_get_indexdef_worker (core worker function)
- Called from (representative examples):
  - Referenced by RULE_INDEXDEF_KEYS_ONLY constant definition (in src/include/utils/ruleutils.h)

## Notes and Other Information
- This function provides the most flexible internal interface for index definition extraction within the ruleutils system
- The flags parameter uses a bitwise approach, allowing multiple options to be combined
- Currently supports two main flag types: formatting control (RULE_INDEXDEF_PRETTY) and content control (RULE_INDEXDEF_KEYS_ONLY)
- The function maintains compatibility with the simpler pg_get_indexdef_columns while providing enhanced functionality
- Returns a palloc'd string that should be freed by the caller when no longer needed
- Part of PostgreSQL's extensible rule utilities framework for object definition formatting