# set_join_column_names

## Location
src/backend/utils/adt/ruleutils.c: 4506 - 4765

## Overview
Selects and assigns column aliases for a join RTE by combining column information from both input relations and handling merged USING columns appropriately.

## Definition
```c
static void set_join_column_names(deparse_namespace *dpns, RangeTblEntry *rte, deparse_columns *colinfo)
```

## Detailed Description
This function handles column alias assignment for join range table entries during rule decompilation. It operates on the assumption that column alias selection has already been completed for both input RTEs and that USING column names have been chosen by set_using_names().

The function performs several key operations:

**Column Name Assignment:**
- Processes non-merged columns starting after any USING columns
- For each column, determines the source (left child, right child, or system column)
- Applies user-written aliases when available, otherwise uses child column names
- Ensures uniqueness through make_colname_unique() for named joins

**New Column Array Construction:**
- Calculates the total number of columns the join would have if re-parsed
- Constructs new_colnames[] and is_new_col[] arrays following parser ordering rules:
  1. Merged columns first (USING clause order)
  2. Non-merged left input columns (attnum order)  
  3. Non-merged right input columns (attnum order)

**Merged Column Handling:**
- Uses bitmapsets to track which child columns are merged via USING clauses
- Skips already-processed merged columns when handling individual child columns
- Maintains proper column ordering even when new columns have been added since parse time

**Alias Printing Logic:**
- Named joins print aliases only if any names were changed from child names
- Unnamed joins never print column aliases (use child names as-is)

## Parameters / Member Variables
- `dpns`: Deparse namespace context containing global naming state
- `rte`: Range table entry for the join being processed
- `colinfo`: Pre-zeroed deparse_columns structure with join shape information already filled by identify_join_columns()

## Dependencies
- Functions called/Symbols referenced:
  - deparse_columns_fetch
  - expand_colnames_array_to
  - make_colname_unique
  - list_nth
  - bms_add_member
  - bms_is_member
- Called from (representative examples):
  - set_deparse_for_query

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for reconstructing SQL text from internal join representations
- Assumes that identify_join_columns() has already been called to populate leftattnos/rightattnos arrays
- Handles the complexity of column ordering when child relations themselves contain new columns added since parse time
- The algorithm must match the parser's column ordering logic to ensure consistent results
- System columns (attnum ≤ 0) are handled specially and use eref names directly
- Critical for maintaining semantic equivalence when decompiling complex join expressions with nested joins and column aliases