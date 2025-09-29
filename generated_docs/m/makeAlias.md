# makeAlias

## Location
[src/backend/nodes/makefuncs.c:436-450](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/makefuncs.c#L436-L450)

## Overview
Creates an Alias node that represents table and column aliases used in SQL queries.

## Definition
```c
Alias *makeAlias(const char *aliasname, List *colnames)
```

## Detailed Description
The `makeAlias` function creates an Alias node that stores alias information for tables, subqueries, and other constructs in SQL. The function allocates a new Alias node and initializes it with the provided alias name and optional column name list. An important implementation detail is that the alias name is copied using pstrdup() for memory safety, while the column names list is used directly without copying.

## Parameters / Member Variables
- `aliasname`: The name of the alias (copied internally for memory safety)
- `colnames`: Optional list of column names for the alias (list is not copied, used directly)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [pstrdup](../p/pstrdup.md)
  - [Alias](../A/Alias.md) (struct type)
- Called from (representative examples):
  - [addRangeTableEntry](../a/addRangeTableEntry.md)
  - [transformInsertStmt](../t/transformInsertStmt.md)
  - [transformSetOperationTree](../t/transformSetOperationTree.md)
  - [addRangeTableEntryForRelation](../a/addRangeTableEntryForRelation.md)
  - [addRangeTableEntryForSubquery](../a/addRangeTableEntryForSubquery.md)

## Notes and Other Information
- The aliasname parameter is copied using pstrdup() to ensure memory safety
- The colnames list parameter is NOT copied - the original list is used directly
- Used extensively throughout the parser and rewriter for managing SQL aliases
- Essential for query processing where table and column renaming is involved
- Memory management consideration: caller must ensure colnames list remains valid for the lifetime of the Alias node

## Simplified Source

```c
Alias *makeAlias(const char *aliasname, List *colnames) {
    // Create new Alias node
    Alias *a = makeNode(Alias);

    // Copy the alias name for memory safety
    a->aliasname = pstrdup(aliasname);

    // Use column names list directly (not copied)
    a->colnames = colnames;

    return a;
}
```