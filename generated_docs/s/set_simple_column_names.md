# set_simple_column_names

## Location
[src/backend/utils/adt/ruleutils.c:4038-4078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L4038-L4078)

## Overview
Fills in column aliases for simple situations like EXPLAIN and cases with only relation RTEs, without complex join tree processing.

## Definition
static void set_simple_column_names(deparse_namespace *dpns)

## Detailed Description
This function provides a simplified approach to column name assignment for scenarios that do not require complex join tree analysis. It is specifically designed for EXPLAIN operations and situations where only relation RTEs are present. The function initializes the rtable_columns structure with zeroed deparse_columns entries and then assigns unique column aliases within each RTE using set_relation_column_names. Unlike the more complex set_deparse_for_query, this function treats all RTEs uniformly, including any join RTEs as simple base relations.

## Parameters / Member Variables
- dpns: Pointer to deparse_namespace structure to have its column names populated

## Dependencies
- Functions called/Symbols referenced:
  - deparse_namespace
  - deparse_columns
  - forboth
  - [set_relation_column_names](set_relation_column_names.md)
- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)
  - [deparse_context_for](../d/deparse_context_for.md)
  - [deparse_context_for_plan_tree](../d/deparse_context_for_plan_tree.md)

## Notes and Other Information
- Designed specifically for EXPLAIN and simple relation-only scenarios
- Does not perform join tree analysis or USING clause processing
- Treats join RTEs as regular base relations if encountered
- EXPLAIN should never encounter join alias Vars, making this simplified approach sufficient
- Creates a one-to-one mapping between rtable entries and rtable_columns entries
- Uses palloc0 to ensure deparse_columns structures are properly zeroed before processing
- More efficient than set_deparse_for_query for cases that don't need full join analysis

## Simplified Source

```c
static void set_simple_column_names(deparse_namespace *dpns) {
    ListCell *lc;
    ListCell *lc2;

    // Initialize rtable_columns with zeroed structs
    dpns->rtable_columns = NIL;
    while (list_length(dpns->rtable_columns) < list_length(dpns->rtable))
        dpns->rtable_columns = lappend(dpns->rtable_columns,
                                      palloc0(sizeof(deparse_columns)));

    // Assign unique column aliases for each RTE
    forboth(lc, dpns->rtable, lc2, dpns->rtable_columns) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        deparse_columns *colinfo = (deparse_columns *) lfirst(lc2);

        set_relation_column_names(dpns, rte, colinfo);
    }
}
```