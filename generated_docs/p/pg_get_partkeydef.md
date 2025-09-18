# pg_get_partkeydef

## Location
src/backend/utils/adt/ruleutils.c: 1889 - 1903

## Overview
A PostgreSQL system function that returns the partition key specification for a partitioned table, including the partitioning method and column details.

## Definition
```c
Datum pg_get_partkeydef(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that generates the textual representation of a table's partition key definition. It extracts and formats the partitioning specification from the system catalogs, producing output that shows the partitioning method (RANGE, LIST, or HASH) along with the partitioning columns and their associated collations and operator classes. The function is designed to provide a complete, human-readable specification that could be used to recreate the partitioning scheme.

The output format follows the pattern:
{ RANGE | LIST | HASH } (column opt_collation opt_opclass [, ...])

This function serves as the SQL-accessible interface for inspecting partition key definitions, commonly used in database administration tools, documentation generation, and schema inspection utilities.

## Parameters / Member Variables
- Uses PG_FUNCTION_ARGS macro which provides:
  - `relid`: The OID of the relation (table) for which to retrieve the partition key definition (retrieved via PG_GETARG_OID(0))

## Dependencies
- Functions called/Symbols referenced:
  - pg_get_partkeydef_worker (the underlying implementation)
  - PRETTYFLAG_INDENT (formatting flag)
  - string_to_text (conversion function)
  - PG_RETURN_TEXT_P (result return macro)
- Called from (representative examples):
  - No direct references found (likely called via SQL interface)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL queries
- Returns NULL if the relation does not exist or is not partitioned
- Uses pretty-printing with indentation for readable output
- The function delegates the actual work to pg_get_partkeydef_worker with specific formatting parameters
- Part of the ruleutils module which handles object definition formatting
- The output includes complete partitioning specification including method, columns, collations, and operator classes
- Commonly used by database administration tools like pg_dump to recreate partition definitions
- The function parameters to the worker (PRETTYFLAG_INDENT, false, true) indicate: use indented formatting, don't include tablespace info, and do include the partitioning method