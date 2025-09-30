# set_join_column_names

## Location
[src/backend/utils/adt/ruleutils.c:4506-4765](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4506-L4765)

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
  - [expand_colnames_array_to](../e/expand_colnames_array_to.md)
  - [make_colname_unique](../m/make_colname_unique.md)
  - [list_nth](../l/list_nth.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_is_member](../b/bms_is_member.md)
- Called from (representative examples):
  - [set_deparse_for_query](set_deparse_for_query.md)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for reconstructing SQL text from internal join representations
- Assumes that identify_join_columns() has already been called to populate leftattnos/rightattnos arrays
- Handles the complexity of column ordering when child relations themselves contain new columns added since parse time
- The algorithm must match the parser's column ordering logic to ensure consistent results
- System columns (attnum ≤ 0) are handled specially and use eref names directly
- Critical for maintaining semantic equivalence when decompiling complex join expressions with nested joins and column aliases

## Simplified Source
```c
static void set_join_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
                                  deparse_columns *colinfo) {
    deparse_columns *leftcolinfo;
    deparse_columns *rightcolinfo;
    bool changed_any = false;
    int noldcolumns;
    int nnewcolumns;
    Bitmapset *leftmerged = NULL;
    Bitmapset *rightmerged = NULL;

    // Get child column info structures
    leftcolinfo = deparse_columns_fetch(colinfo->leftrti, dpns);
    rightcolinfo = deparse_columns_fetch(colinfo->rightrti, dpns);

    // Ensure colnames array has enough slots
    noldcolumns = list_length(rte->eref->colnames);
    expand_colnames_array_to(colinfo, noldcolumns);

    // Process non-merged columns (USING columns already handled)
    for (int i = list_length(colinfo->usingNames); i < noldcolumns; i++) {
        char *colname = colinfo->colnames[i];
        char *real_colname;

        // Get source column name (left, right, or system column)
        if (colinfo->leftattnos[i] > 0)
            real_colname = leftcolinfo->colnames[colinfo->leftattnos[i] - 1];
        else if (colinfo->rightattnos[i] > 0)
            real_colname = rightcolinfo->colnames[colinfo->rightattnos[i] - 1];
        else
            real_colname = strVal(list_nth(rte->eref->colnames, i));

        // Skip dropped columns
        if (real_colname == NULL) {
            colinfo->colnames[i] = NULL;
            continue;
        }

        // For unnamed joins, use child names directly
        if (rte->alias == NULL) {
            colinfo->colnames[i] = real_colname;
            continue;
        }

        // Assign alias if not already set
        if (colname == NULL) {
            // Use user alias if provided, otherwise use real name
            if (rte->alias && i < list_length(rte->alias->colnames))
                colname = strVal(list_nth(rte->alias->colnames, i));
            else
                colname = real_colname;

            // Make unique and store
            colname = make_colname_unique(colname, dpns, colinfo);
            colinfo->colnames[i] = colname;
        }

        // Track if any names changed
        if (!changed_any && strcmp(colname, real_colname) != 0)
            changed_any = true;
    }

    // Calculate total columns for new arrays
    nnewcolumns = leftcolinfo->num_new_cols + rightcolinfo->num_new_cols -
                  list_length(colinfo->usingNames);
    colinfo->num_new_cols = nnewcolumns;
    colinfo->new_colnames = (char **) palloc0(nnewcolumns * sizeof(char *));
    colinfo->is_new_col = (bool *) palloc0(nnewcolumns * sizeof(bool));

    // Build new column arrays in parser order: merged, left, right
    int i = 0, j = 0;

    // Handle merged columns first
    while (i < noldcolumns &&
           colinfo->leftattnos[i] != 0 && colinfo->rightattnos[i] != 0) {
        colinfo->new_colnames[j] = colinfo->colnames[i];
        colinfo->is_new_col[j] = false;

        // Track merged column positions
        if (colinfo->leftattnos[i] > 0)
            leftmerged = bms_add_member(leftmerged, colinfo->leftattnos[i]);
        if (colinfo->rightattnos[i] > 0)
            rightmerged = bms_add_member(rightmerged, colinfo->rightattnos[i]);

        i++, j++;
    }

    // Handle left child columns (simplified logic)
    for (int jc = 0; jc < leftcolinfo->num_new_cols; jc++) {
        // Skip merged columns, assign names for non-merged ones
        // For new columns, unique-ify names unless unnamed join
        if (rte->alias != NULL && leftcolinfo->is_new_col[jc]) {
            colinfo->new_colnames[j] =
                make_colname_unique(leftcolinfo->new_colnames[jc], dpns, colinfo);
        } else {
            colinfo->new_colnames[j] = leftcolinfo->new_colnames[jc];
        }
        colinfo->is_new_col[j] = leftcolinfo->is_new_col[jc];
        j++;
    }

    // Handle right child columns (same logic as left)
    for (int jc = 0; jc < rightcolinfo->num_new_cols; jc++) {
        if (rte->alias != NULL && rightcolinfo->is_new_col[jc]) {
            colinfo->new_colnames[j] =
                make_colname_unique(rightcolinfo->new_colnames[jc], dpns, colinfo);
        } else {
            colinfo->new_colnames[j] = rightcolinfo->new_colnames[jc];
        }
        colinfo->is_new_col[j] = rightcolinfo->is_new_col[jc];
        j++;
    }

    // Set alias printing flag
    colinfo->printaliases = (rte->alias != NULL) ? changed_any : false;
}
```