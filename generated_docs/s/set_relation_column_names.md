# set_relation_column_names

## Location
[src/backend/utils/adt/ruleutils.c:4310-4505](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4310-L4505)

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
  - [relation_open](../r/relation_open.md)
  - [relation_close](../r/relation_close.md)  
  - [expandRTE](../e/expandRTE.md)
  - [expand_colnames_array_to](../e/expand_colnames_array_to.md)
  - [make_colname_unique](../m/make_colname_unique.md)
  - [list_nth](../l/list_nth.md)
  - TupleDescAttr
  - RelationGetDescr
- Called from (representative examples):
  - [set_deparse_for_query](set_deparse_for_query.md)
  - [set_simple_column_names](set_simple_column_names.md)

## Notes and Other Information
- Part of PostgreSQL's rule decompilation system for converting internal representations back to SQL text
- Handles the complexity of dropped columns which can occur between query parse time and decompilation
- Different printing strategies are used based on RTE type: relations print aliases only when changed, functions always print complete alias lists, tablefunc never prints aliases
- The function accounts for columns that may have been added since the original query was parsed
- Maintains backward compatibility by preserving user-written column aliases when available
- Critical for ensuring that decompiled rules and views remain syntactically correct and semantically equivalent

## Simplified Source

```c
static void set_relation_column_names(deparse_namespace *dpns, RangeTblEntry *rte,
                                      deparse_columns *colinfo) {
    int ncolumns;
    char **real_colnames;
    bool changed_any;
    int noldcolumns;

    // Get current column names based on RTE type
    if (rte->rtekind == RTE_RELATION) {
        // For tables/views: get up-to-date info from system catalogs
        Relation rel = relation_open(rte->relid, AccessShareLock);
        TupleDesc tupdesc = RelationGetDescr(rel);

        ncolumns = tupdesc->natts;
        real_colnames = (char **) palloc(ncolumns * sizeof(char *));

        for (int i = 0; i < ncolumns; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped)
                real_colnames[i] = NULL;
            else
                real_colnames[i] = pstrdup(NameStr(attr->attname));
        }
        relation_close(rel, AccessShareLock);
    } else {
        // For functions and other RTEs: use eref or expandRTE
        List *colnames;
        if (rte->rtekind == RTE_FUNCTION && rte->functions != NIL) {
            expandRTE(rte, 1, 0, -1, true, &colnames, NULL);
        } else {
            colnames = rte->eref->colnames;
        }

        ncolumns = list_length(colnames);
        real_colnames = (char **) palloc(ncolumns * sizeof(char *));

        int i = 0;
        ListCell *lc;
        foreach(lc, colnames) {
            char *cname = strVal(lfirst(lc));
            real_colnames[i] = (cname[0] == '\0') ? NULL : cname;
            i++;
        }
    }

    // Prepare colinfo arrays
    expand_colnames_array_to(colinfo, ncolumns);
    colinfo->new_colnames = (char **) palloc(ncolumns * sizeof(char *));
    colinfo->is_new_col = (bool *) palloc(ncolumns * sizeof(bool));

    // Assign unique aliases for each column
    noldcolumns = list_length(rte->eref->colnames);
    changed_any = false;
    int j = 0;

    for (int i = 0; i < ncolumns; i++) {
        char *real_colname = real_colnames[i];
        char *colname = colinfo->colnames[i];

        // Skip dropped columns
        if (real_colname == NULL)
            continue;

        // Determine column alias (user alias or real name)
        if (colname == NULL) {
            if (rte->alias && i < list_length(rte->alias->colnames))
                colname = strVal(list_nth(rte->alias->colnames, i));
            else
                colname = real_colname;

            colname = make_colname_unique(colname, dpns, colinfo);
            colinfo->colnames[i] = colname;
        }

        // Track non-dropped columns
        colinfo->new_colnames[j] = colname;
        colinfo->is_new_col[j] = (i >= noldcolumns);
        j++;

        // Check if any aliases differ from real names
        if (!changed_any && strcmp(colname, real_colname) != 0)
            changed_any = true;
    }

    colinfo->num_new_cols = j;

    // Determine whether to print aliases based on RTE type
    if (rte->rtekind == RTE_RELATION)
        colinfo->printaliases = changed_any;
    else if (rte->rtekind == RTE_FUNCTION)
        colinfo->printaliases = true;
    else if (rte->rtekind == RTE_TABLEFUNC)
        colinfo->printaliases = false;
    else
        colinfo->printaliases = changed_any || (rte->alias && rte->alias->colnames != NIL);
}
```