# pg_get_indexdef_columns

## Location
src/backend/utils/adt/ruleutils.c: 1215 - 1228

## Overview
Returns the key-column definitions of a PostgreSQL index as a formatted string, providing an internal interface for extracting just the column specification portion of an index definition.

## Definition


## Detailed Description
This function serves as an internal wrapper around pg_get_indexdef_worker to specifically extract and format the key-column definitions of an index. It focuses solely on the column specifications without including other index attributes like the index name, table name, or access method. The function sets specific flags to pg_get_indexdef_worker to ensure only key columns are returned in the output.

The function processes the pretty formatting flag to determine output formatting and delegates the actual work to pg_get_indexdef_worker with predetermined parameters that focus on key columns only.

## Parameters / Member Variables
- : The OID of the index relation for which to retrieve column definitions
- : Boolean flag indicating whether to format the output with pretty formatting (affects spacing and line breaks)

## Dependencies
- Functions called/Symbols referenced:
  - GET_PRETTY_FLAGS (macro for converting boolean to formatting flags)
  - [pg_get_indexdef_worker](pg_get_indexdef_worker.md) (core worker function that performs the actual index definition extraction)
- Called from (representative examples):
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md) (in src/backend/access/index/genam.c)
  - Used via RULE_INDEXDEF_KEYS_ONLY constant (in src/include/utils/ruleutils.h)

## Notes and Other Information
- This is specifically designed as an internal version that reports only key-column definitions
- The function passes hardcoded boolean parameters to pg_get_indexdef_worker: (true, true, false, false) which control what parts of the index definition are included
- The function is part of PostgreSQL's rule utilities system, which handles the formatting and display of database object definitions
- Returns a palloc'd string that should be freed by the caller when no longer needed