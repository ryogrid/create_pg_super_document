# make_path_rowexpr

## Location
[src/backend/rewrite/rewriteSearchCycle.c:117-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteSearchCycle.c#L117-L158)

## Overview
Creates a RowExpr node from specified column names for use in PostgreSQL's CTE (Common Table Expression) SEARCH and CYCLE clause rewriting.

## Definition

```c
static RowExpr *
make_path_rowexpr(const CommonTableExpr *cte, const List *col_list)
```
## Detailed Description
This static function constructs a RowExpr node that represents a row expression containing variables corresponding to the specified column names from a CTE. It's used internally by the CTE rewriting mechanism to create row expressions for SEARCH and CYCLE clauses. The function iterates through the provided column list, matches each column name against the CTE's column names, and creates corresponding Var nodes that reference the appropriate columns by position and type information.

The resulting RowExpr has a record type (RECORDOID) and uses implicit coercion format, making it suitable for use in row comparisons and array operations within the rewritten CTE queries.

## Parameters / Member Variables
- : Pointer to the CommonTableExpr containing the CTE definition with column names, types, and metadata
- : List of column names (as String nodes) to include in the row expression

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create RowExpr)
  - strVal (to extract string values)
  - [list_length](../l/list_length.md) (to get list size)
  - [list_nth](../l/list_nth.md) (to access list elements)
  - [makeVar](makeVar.md) (to create variable references)
  - [list_nth_oid](../l/list_nth_oid.md) (to get OID from list)
  - [list_nth_int](../l/list_nth_int.md) (to get integer from list)
  - [lappend](../l/lappend.md) (to append to lists)
  - [makeString](makeString.md) (to create String nodes)
- Called from:
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md) (at lines 319 and 343)

## Notes and Other Information
- This is a static helper function only used within rewriteSearchCycle.c
- The function assumes all specified column names exist in the CTE's column list
- Row expressions created by this function are used to construct the path tracking mechanisms for recursive CTE traversal in both breadth-first and depth-first search scenarios
- The location field is set to -1 indicating no specific source location for the constructed node