# explain_get_index_name

## Location
src/backend/commands/explain.c: 3679 - 3702

## Overview
Retrieves the name of an index for EXPLAIN output, with support for plugin hooks to handle hypothetical indexes.

## Definition
```c
static const char *explain_get_index_name(Oid indexId)
```

## Detailed Description
This function provides a pluggable mechanism for obtaining index names during EXPLAIN operations. It first checks if a plugin hook (explain_get_index_name_hook) is installed and calls it to allow custom handling of index name resolution. This is particularly useful for hypothetical indexes created by extensions or plugins that don't exist in the system catalogs.

If no hook is installed or the hook returns NULL, the function falls back to the standard behavior of looking up the index name in the system catalogs using get_rel_name(). The function ensures that a valid name is always returned, raising an error if the index cannot be found.

The returned names are intentionally "raw" (unquoted) to allow the caller to apply appropriate quoting based on the output format being used.

## Parameters / Member Variables
- `indexId`: OID of the index whose name should be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - explain_get_index_name_hook (global function pointer, if set)
  - get_rel_name
  - elog
- Called from (representative examples):
  - ExplainNode
  - ExplainIndexScanDetails

## Notes and Other Information
- The hook mechanism allows extensions to provide names for hypothetical or temporary indexes that don't exist in pg_class
- Names returned are unquoted/raw to allow proper formatting in different output formats (TEXT, JSON, XML, YAML)
- Raises an ERROR if the index OID cannot be resolved to a name, indicating a potential catalog corruption or timing issue
- This function is part of PostgreSQL's extensible EXPLAIN infrastructure