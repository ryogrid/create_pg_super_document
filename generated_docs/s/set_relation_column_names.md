# set_relation_column_names

## Location
src/backend/utils/adt/ruleutils.c: 4310 - 4505

## Overview
Selects and assigns unique column aliases for a non-join RTE (Range Table Entry) by examining the actual column names and creating appropriate aliases for rule decompilation.

## Definition
```c
static void set_relation_column_names(deparse_namespace *dpns, RangeTblEntry *rte, deparse_columns *colinfo)
```

## Detailed Description
This function handles the selection of column aliases for non-join range table entries during rule decompilation. It constructs an array of current "real" column names and assigns unique aliases for each column, handling various RTE types differently:

**For RTE_RELATION (tables/views):**
- Opens the relation and retrieves up-to-date column information from system catalogs
- Handles dropped columns by setting their entries to NULL
- Uses relation_open/relation_close to access tuple descriptor information

**For RTE_FUNCTION with available functions:**
- Uses expandRTE() to handle potentially dropped columns in composite return types
- Falls back to rte->eref when function information is unavailable (e.g., during EXPLAIN)

**For other RTE types:**
- Uses rte->eref->colnames which should be sufficiently current

The function manages two parallel arrays: colnames[] (includes NULLs for dropped columns) and new_colnames[] (omits dropped columns). It also tracks whether columns are new since parse time and determines when column aliases need to be printed based on the RTE type and whether any names have changed.

## Parameters / Member Variables
- `dpns`: Deparse namespace context containing global naming state and uniqueness tracking
- `rte`: Range table entry for the relation being processed  
- `colinfo`: Pre-zeroed deparse_columns structure to be filled with column naming information

## Dependencies
- Functions called/Symbols referenced:
  - relation_open
  - relation_close  
  - expandRTE
  - expand_colnames_array_to
  - make_colname_unique
  - list_nth
  - TupleDescAttr
  - RelationGetDescr
- Called from (representative examples):
  - set_deparse_for_query
  - set_simple_column_names

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for converting internal representations back to SQL text
- Handles the complexity of dropped columns which can occur between query parse time and decompilation
- Different printing strategies are used based on RTE type: relations print aliases only when changed, functions always print complete alias lists, tablefunc never prints aliases
- The function accounts for columns that may have been added since the original query was parsed
- Maintains backward compatibility by preserving user-written column aliases when available
- Critical for ensuring that decompiled rules and views remain syntactically correct and semantically equivalent