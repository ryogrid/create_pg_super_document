# IsCurrentOfClause

## Location
[src/backend/optimizer/path/tidpath.c:211-233](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/tidpath.c#L211-L233)

## Overview
A static helper function that determines whether a given RestrictInfo represents a CurrentOfExpr that references a specific relation.

## Definition

```c
static bool
IsCurrentOfClause(RestrictInfo *rinfo, RelOptInfo *rel)
```
## Detailed Description
IsCurrentOfClause is a utility function used in PostgreSQL's query optimizer to identify CURRENT OF clauses in WHERE conditions. It checks if a restriction clause is a CurrentOfExpr (which represents "WHERE CURRENT OF cursor_name" clauses) and whether that clause references the specified relation. This function is part of the TID (tuple identifier) path planning logic, which optimizes queries that can directly access tuples by their physical locations.

The function performs two key validations:
1. Verifies that the restriction clause is indeed a CurrentOfExpr node type
2. Confirms that the CurrentOfExpr references the target relation by comparing relation IDs

## Parameters / Member Variables
- `*rinfo`: RestrictInfo pointer containing the clause to examine
- `*rel`: RelOptInfo pointer representing the relation being checked against
## Dependencies
- Functions called/Symbols referenced:
  - [CurrentOfExpr](../C/CurrentOfExpr.md) (node type)
  - IsA (macro for type checking)
- Called from (representative examples):
  - [RestrictInfoIsTidQual](../R/RestrictInfoIsTidQual.md)
  - [TidQualFromRestrictInfoList](../T/TidQualFromRestrictInfoList.md)

## Notes and Other Information
- This is a static function, so it's only accessible within the tidpath.c file
- CURRENT OF clauses are used with cursors to reference the current row position
- The function returns false if the clause is not a CurrentOfExpr or doesn't reference the specified relation
- Part of PostgreSQL's TID scan optimization infrastructure for direct tuple access

## Simplified Source

```c
static bool
IsCurrentOfClause(RestrictInfo *rinfo, RelOptInfo *rel)
{
    CurrentOfExpr *node;

    // Check if clause is a CurrentOfExpr
    if (!(rinfo->clause && IsA(rinfo->clause, CurrentOfExpr)))
        return false;

    node = (CurrentOfExpr *) rinfo->clause;

    // Check if it references this relation
    if (node->cvarno == rel->relid)
        return true;

    return false;
}
```