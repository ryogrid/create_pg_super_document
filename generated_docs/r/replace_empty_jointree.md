# replace_empty_jointree

## Location
[src/backend/optimizer/prep/prepjointree.c:395-452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L395-L452)

## Overview
Replaces an empty query jointree with a dummy RTE_RESULT relation to avoid corner cases in query processing.

## Definition

```c
void
replace_empty_jointree(Query *parse)
```
## Detailed Description
This function addresses a common issue with SELECTs that have omitted FROM clauses by ensuring that every query has at least one relation in its jointree. When a query's jointree is empty (no FROM clause), it creates a synthetic RTE_RESULT relation and adds it to the jointree. This approach eliminates numerous corner cases that previously existed in query processing.

The function helps with scenarios such as subquery pull-up operations, where an empty relid set would make the subquery not uniquely identifiable for join or PlaceHolderVar processing. By ensuring a non-empty jointree, these operations can proceed normally.

Unlike most other functions in prepjointree.c, this function does not recurse on sub-queries, relying on other processing stages to invoke it at appropriate times.

## Parameters / Member Variables
- `*parse`: The Query structure whose jointree may need to be replaced if empty
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for RangeTblEntry, RangeTblRef)
  - [makeAlias](../m/makeAlias.md)
  - [lappend](../l/lappend.md)
  - [list_length](../l/list_length.md)
  - list_make1
  - RTE_RESULT constant
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (in src/backend/optimizer/plan/planner.c:706)
  - [convert_EXISTS_sublink_to_join](../c/convert_EXISTS_sublink_to_join.md) (in src/backend/optimizer/plan/subselect.c:1442)
  - [pull_up_simple_subquery](../p/pull_up_simple_subquery.md) (in src/backend/optimizer/prep/prepjointree.c:1191)

## Notes and Other Information
- Only operates on queries with completely empty fromlist in their jointree
- Does not modify queries that are part of set operations (UNION, INTERSECT, EXCEPT)
- Creates a synthetic RTE with eref alias "*RESULT*"
- The RTE_RESULT relation type represents a relation that produces rows without scanning any actual table
- This transformation is essential for proper functioning of the query optimizer's join processing and subquery handling mechanisms
- Non-recursive design requires careful coordination with other query processing phases

## Simplified Source

```c
void replace_empty_jointree(Query *parse) {
    // Skip if jointree already has relations or is part of set operations
    if (parse->jointree->fromlist != NIL || parse->setOperations)
        return;

    // Create a dummy RTE_RESULT relation
    RangeTblEntry *rte = makeNode(RangeTblEntry);
    rte->rtekind = RTE_RESULT;
    rte->eref = makeAlias("*RESULT*", NIL);

    // Add to range table and get its index
    parse->rtable = lappend(parse->rtable, rte);
    Index rti = list_length(parse->rtable);

    // Create reference and add to jointree
    RangeTblRef *rtr = makeNode(RangeTblRef);
    rtr->rtindex = rti;
    parse->jointree->fromlist = list_make1(rtr);
}
```