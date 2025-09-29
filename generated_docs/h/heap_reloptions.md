# heap_reloptions

## Location
[src/backend/access/common/reloptions.c:2028-2062](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L2028-L2062)

## Overview
Parses and validates relation options for heap tables, materialized views, and TOAST tables, providing relkind-specific option handling with specialized defaults for TOAST relations.

## Definition

```c
bytea *
heap_reloptions(char relkind, Datum reloptions, bool validate)
```
## Detailed Description
The `heap_reloptions` function is a central option parser that handles relation options for different types of heap-based relations in PostgreSQL. It uses a switch statement based on the relation kind (relkind) to provide specialized option parsing. For TOAST tables (RELKIND_TOASTVALUE), it applies special defaults including setting fillfactor to 100% and disabling analyze operations by setting negative thresholds. For regular tables (RELKIND_RELATION) and materialized views (RELKIND_MATVIEW), it delegates to the standard `default_reloptions` function with RELOPT_KIND_HEAP. The function ensures that each relation type gets appropriate default values and validation rules for their specific use cases.

## Parameters / Member Variables
- `relkind`: Character indicating the relation kind (table, TOAST table, materialized view, etc.)
- `reloptions`: Datum containing the raw relation options to be parsed and processed
- `validate`: Boolean flag indicating whether to perform validation of the option values during parsing

## Dependencies
- Functions called/Symbols referenced:
  - [default_reloptions](../d/default_reloptions.md)
  - [StdRdOptions](../S/StdRdOptions.md) (structure)
  - RELKIND_TOASTVALUE (constant)
  - RELKIND_RELATION (constant)
  - RELKIND_MATVIEW (constant)
  - RELOPT_KIND_TOAST (constant)
  - RELOPT_KIND_HEAP (constant)
- Called from (representative examples):
  - [extractRelOptions](../e/extractRelOptions.md)
  - [create_ctas_internal](../c/create_ctas_internal.md)
  - [DefineRelation](../D/DefineRelation.md)
  - [ATExecSetRelOptions](../A/ATExecSetRelOptions.md)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- TOAST tables receive special treatment with fillfactor set to 100% for optimal space utilization since TOAST data is typically accessed sequentially
- TOAST tables have autovacuum analyze operations disabled (thresholds set to -1) since analyze statistics are not meaningful for TOAST data
- Regular heap tables and materialized views use the same option parsing via RELOPT_KIND_HEAP
- The function returns NULL for unsupported relation kinds, ensuring type safety
- This function is a key component in PostgreSQL's storage parameter system, allowing users to tune performance characteristics per relation type

## Simplified Source
```c
/*
 * Parse options for heaps, views and toast tables.
 */
bytea *
heap_reloptions(char relkind, Datum reloptions, bool validate)
{
    StdRdOptions *rdopts;

    switch (relkind)
    {
        case RELKIND_TOASTVALUE:
            rdopts = (StdRdOptions *)
                default_reloptions(reloptions, validate, RELOPT_KIND_TOAST);
            if (rdopts != NULL)
            {
                /* adjust default-only parameters for TOAST relations */
                rdopts->fillfactor = 100;
                rdopts->autovacuum.analyze_threshold = -1;
                rdopts->autovacuum.analyze_scale_factor = -1;
            }
            return (bytea *) rdopts;
        case RELKIND_RELATION:
        case RELKIND_MATVIEW:
            return default_reloptions(reloptions, validate, RELOPT_KIND_HEAP);
        default:
            /* other relkinds are not supported */
            return NULL;
    }
}
```