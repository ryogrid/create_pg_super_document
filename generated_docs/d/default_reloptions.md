# default_reloptions

## Location
[src/backend/access/common/reloptions.c:1847-1916](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1847-L1916)

## Overview
A function that provides standardized parsing of relation options for tables that use the StdRdOptions structure, handling common table storage parameters and autovacuum settings.

## Definition

```c
bytea *
default_reloptions(Datum reloptions, bool validate, relopt_kind kind)
```
## Detailed Description
This function serves as an option parser for any relation type that uses the standard StdRdOptions structure. It defines a comprehensive parsing table that includes storage parameters like fillfactor, toast settings, parallel worker configuration, vacuum settings, and detailed autovacuum options. The function delegates the actual parsing and structure building to build_reloptions, providing it with the StdRdOptions-specific parsing table and structure size.

## Parameters / Member Variables
- : Input Datum containing the raw relation options to be parsed
- : Boolean flag indicating whether to validate all provided options against the parsing table
- : The specific kind of relation options being processed (relopt_kind enum)

## Dependencies
- Functions called/Symbols referenced:
  - relopt_kind (enum type)
  - relopt_parse_elt (struct type)
  - StdRdOptions (struct type)
  - AutoVacOpts (struct type)
  - RELOPT_TYPE_INT, RELOPT_TYPE_BOOL, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM (enum values)
  - [build_reloptions](../b/build_reloptions.md) (function)
  - lengthof (macro)
  - offsetof (macro)
- Called from:
  - [heap_reloptions](../h/heap_reloptions.md) (src/backend/access/common/reloptions.c:2036)
  - [heap_reloptions](../h/heap_reloptions.md) (src/backend/access/common/reloptions.c:2047)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:236)

## Notes and Other Information
- Supports 22 different standard relation options including fillfactor, toast_tuple_target, parallel_workers, user_catalog_table, vacuum_index_cleanup, vacuum_truncate, and 16 autovacuum-related parameters
- The autovacuum options cover thresholds, scale factors, cost parameters, freeze ages for both regular and multixact transactions, and logging settings
- Uses offsetof macro extensively to calculate field positions within nested structures (StdRdOptions containing AutoVacOpts)
- Returns a bytea pointer containing the parsed and structured option data
- This function provides the foundation for standard table option parsing used by heap tables and other relation types that follow the standard options pattern