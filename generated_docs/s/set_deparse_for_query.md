# set_deparse_for_query

## Location
[src/backend/utils/adt/ruleutils.c:3973-4037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L3973-L4037)

## Overview
Sets up a complete deparse_namespace structure for deparsing a Query tree, initializing all necessary components for query reconstruction.

## Definition
static void set_deparse_for_query(deparse_namespace *dpns, Query *query, List *parent_namespaces)

## Detailed Description
This function provides a comprehensive initialization of a deparse_namespace structure from a Query tree. It performs a complete setup including relation table links, CTE links, relation alias assignment, column structure initialization, and column name assignment. The function handles both regular queries and utility queries, and includes special processing for JOIN operations using USING clauses. It ensures that all necessary namespace information is available for subsequent query deparsing operations.

## Parameters / Member Variables
- dpns: Pointer to deparse_namespace structure to be initialized (zeroed and populated)
- query: Query tree containing the source information for namespace setup
- parent_namespaces: List of ancestor namespace contexts for name conflict resolution

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace
  - [set_rtable_names](set_rtable_names.md)
  - deparse_columns
  - [has_dangerous_join_using](../h/has_dangerous_join_using.md)
  - [set_using_names](set_using_names.md)
  - [set_join_column_names](set_join_column_names.md)
  - [set_relation_column_names](set_relation_column_names.md)
  - forboth
- Called from (representative examples):
  - [make_ruledef](../m/make_ruledef.md)
  - [get_query_def](../g/get_query_def.md)
  - [get_name_for_var_field](../g/get_name_for_var_field.md)

## Notes and Other Information
- Initializes the deparse_namespace struct from scratch using memset
- Links rtable and cteList directly from the query structure
- Creates zeroed deparse_columns structures for each RTE in rtable_columns
- Handles utility queries that lack a jointree by checking for NULL
- Processes USING clause name conflicts through has_dangerous_join_using detection
- Uses recursive processing for USING names via set_using_names
- Processes RTEs in linear rtable order to handle all relations including NEW.* and INSERT targets
- Ensures JOIN RTEs are processed after their children due to rtable ordering

## Simplified Source

```c
static void set_deparse_for_query(deparse_namespace *dpns, Query *query, List *parent_namespaces) {
    // Initialize deparse namespace structure
    memset(dpns, 0, sizeof(deparse_namespace));
    dpns->rtable = query->rtable;
    dpns->ctes = query->cteList;
    dpns->subplans = NIL;
    dpns->appendrels = NULL;

    // Assign unique aliases to each relation table entry
    set_rtable_names(dpns, parent_namespaces, NULL);

    // Create column info structures for each RTE
    dpns->rtable_columns = NIL;
    while (list_length(dpns->rtable_columns) < list_length(dpns->rtable))
        dpns->rtable_columns = lappend(dpns->rtable_columns, palloc0(sizeof(deparse_columns)));

    // Process query jointure if it exists (not utility queries)
    if (query->jointree) {
        // Check for dangerous USING name conflicts
        dpns->unique_using = has_dangerous_join_using(dpns, (Node *) query->jointree);

        // Set column names for USING clauses
        set_using_names(dpns, (Node *) query->jointree, NIL);
    }

    // Assign column names for all RTEs (including non-jointree relations)
    forboth(lc, dpns->rtable, lc2, dpns->rtable_columns) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        deparse_columns *colinfo = (deparse_columns *) lfirst(lc2);

        if (rte->rtekind == RTE_JOIN)
            set_join_column_names(dpns, rte, colinfo);
        else
            set_relation_column_names(dpns, rte, colinfo);
    }
}
```