# heap_reloptions

## Location
src/backend/access/common/reloptions.c: 2028 - 2062

## Overview
Parses and validates relation options for heap tables, materialized views, and TOAST tables, providing relkind-specific option handling with specialized defaults for TOAST relations.

## Definition


## Detailed Description
The `heap_reloptions` function is a central option parser that handles relation options for different types of heap-based relations in PostgreSQL. It uses a switch statement based on the relation kind (relkind) to provide specialized option parsing. For TOAST tables (RELKIND_TOASTVALUE), it applies special defaults including setting fillfactor to 100% and disabling analyze operations by setting negative thresholds. For regular tables (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW), it delegates to the standard `default_reloptions` function with RELOPT_KIND_HEAP. The function ensures that each relation type gets appropriate default values and validation rules for their specific use cases.

## Parameters / Member Variables
- `relkind`: Character indicating the relation kind (table, TOAST table, materialized view, etc.)
- `reloptions`: Datum containing the raw relation options to be parsed and processed
- `validate`: Boolean flag indicating whether to perform validation of the option values during parsing

## Dependencies
- Functions called/Symbols referenced:
  - default_reloptions
  - StdRdOptions (structure)
  - RELKIND_TOASTVALUE (constant)
  - RELKIND_RELATION (constant)
  - RELKIND_MATVIEW (constant)
  - RELOPT_KIND_TOAST (constant)
  - RELOPT_KIND_HEAP (constant)
- Called from (representative examples):
  - extractRelOptions
  - create_ctas_internal
  - DefineRelation
  - ATExecSetRelOptions
  - ProcessUtilitySlow

## Notes and Other Information
- TOAST tables receive special treatment with fillfactor set to 100% for optimal space utilization since TOAST data is typically accessed sequentially
- TOAST tables have autovacuum analyze operations disabled (thresholds set to -1) since analyze statistics are not meaningful for TOAST data
- Regular heap tables and materialized views use the same option parsing via RELOPT_KIND_HEAP
- The function returns NULL for unsupported relation kinds, ensuring type safety
- This function is a key component in PostgreSQL's storage parameter system, allowing users to tune performance characteristics per relation type