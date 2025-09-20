# tsvector_update_trigger_bycolumn

## Location
[src/backend/utils/adt/tsvector_op.c:2733-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2733-L2738)

## Overview
A PostgreSQL trigger function that automatically updates a tsvector column based on text column(s), using a text search configuration specified by name rather than column ID.

## Definition

```c
Datum
tsvector_update_trigger_bycolumn(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a database trigger function designed to automatically maintain tsvector columns when related text columns are modified. This function is a wrapper that calls the main  implementation with the  parameter, indicating that the text search configuration should be identified by a literal configuration name rather than by a regconfig column ID.

This trigger function is typically used in scenarios where a fixed, predetermined text search configuration should be applied to all rows in the table. The configuration name must be explicitly schema-qualified to avoid ambiguity. This approach provides consistent text processing behavior across all rows in the table.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing trigger-specific arguments:
  - Argument 0: Name of the tsvector column to update
  - Argument 1: Name of the text search configuration (must be schema-qualified)
  - Remaining arguments: Names of text columns to be processed into the tsvector

## Dependencies
- Functions called/Symbols referenced:
  -  - Main trigger implementation function (called with )
- Called from (representative examples):
  - No direct references found (likely called via PostgreSQL trigger system)

## Notes and Other Information
- This function is specifically designed to be used as a PostgreSQL trigger function
- The second argument must be a literal text search configuration name, not a column reference
- The configuration name must be schema-qualified (e.g., 'pg_catalog.english') for security and clarity
- Part of PostgreSQL's automatic tsvector maintenance system for full-text search
- The  parameter passed to the main function indicates that configuration lookup should be by name
- Trigger arguments follow a specific pattern: tsvector_column, config_name, text_column1, [text_column2, ...]
- Provides consistent text search processing across all rows using the same configuration
- Automatically called when INSERT or UPDATE operations occur on the table where this trigger is installed
- More efficient than the byid variant when all rows use the same configuration since no per-row lookups are needed