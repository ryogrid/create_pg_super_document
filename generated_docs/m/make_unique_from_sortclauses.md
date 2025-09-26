# make_unique_from_sortclauses

## Location
[src/backend/optimizer/plan/createplan.c:6700-6748](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/createplan.c#L6700-L6748)

## Overview
Creates a Unique plan node from a list of SortGroupClauses to eliminate duplicate rows based on specified columns from pre-sorted input.

## Definition
```c
static Unique *make_unique_from_sortclauses(Plan *lefttree, List *distinctList)
```

## Detailed Description
This static function constructs a Unique plan node that removes duplicate rows from sorted input data. It takes a list of SortGroupClauses that specify which columns should be considered for uniqueness filtering and converts them into the array-based format expected by the executor. The input must already be sorted according to the specified distinct columns for the Unique node to work correctly. The function extracts attribute numbers, equality operators, and collations from each SortGroupClause and stores them in parallel arrays for efficient execution-time processing.

## Parameters / Member Variables
- `lefttree`: Left child plan node providing sorted input tuples
- `distinctList`: List of SortGroupClauses identifying targetlist items for uniqueness comparison

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create Unique node)
  - [list_length](../l/list_length.md) (to get number of distinct columns)
  - [palloc](../p/palloc.md) (to allocate arrays for column information)
  - [get_sortgroupclause_tle](../g/get_sortgroupclause_tle.md) (to find target entry for each sort group clause)
  - [exprCollation](../e/exprCollation.md) (to extract collation from target entry expression)
- Types referenced:
  - [Unique](../U/Unique.md) (the uniqueness filtering plan node structure)
  - [SortGroupClause](../S/SortGroupClause.md) (structure defining sorting/grouping criteria)
  - [TargetEntry](../T/TargetEntry.md) (structure representing output columns)
- Called from (representative examples):
  - [create_unique_plan](../c/create_unique_plan.md)

## Notes and Other Information
- This is a static function, only accessible within the createplan.c file
- Input must be pre-sorted on the distinct columns for correct operation
- Converts high-level SortGroupClause representation to low-level arrays for executor efficiency
- Uses equality operators from SortGroupClauses to determine when rows are duplicates
- Allocates memory for three parallel arrays: column indexes, operators, and collations
- The function assumes that all equality operators in the SortGroupClauses are valid (asserts OidIsValid)
- No qualification conditions (WHERE clauses) are applied - the node only performs deduplication
- The right child plan node is always set to NULL as uniqueness filtering is a unary operation
- Commonly used to implement DISTINCT clauses in SQL queries when sort-based deduplication is chosen over hash-based approaches