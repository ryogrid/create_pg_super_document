# findAttrByName

## Location
[src/backend/commands/tablecmds.c:3478-3514](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L3478-L3514)

## Overview
findAttrByName searches for an existing column entry with a specified name in a list of ColumnDef structures and returns its position index.

## Definition

```c
static int
findAttrByName(const char *attributeName, const List *columns)
```
## Detailed Description
This is a utility function that performs a linear search through a list of ColumnDef structures to locate a column with a specific name. The function uses string comparison to match column names and returns the 1-based index position of the first matching column found.

The search is case-sensitive and uses standard C string comparison (strcmp). The function follows PostgreSQL's convention of using 1-based indexing for attribute numbers, which aligns with how PostgreSQL internally numbers table columns.

## Parameters / Member Variables
- : The name of the column to search for (null-terminated C string)
- : List of ColumnDef structures to search through

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - lfirst_node
  - [ColumnDef](../C/ColumnDef.md) (structure type)
- Called from (representative examples):
  - [MergeAttributes](../M/MergeAttributes.md)

## Notes and Other Information
- Returns 1-based index (1, 2, 3, ...) when column is found, 0 when not found
- Performs linear search with O(n) time complexity
- Uses case-sensitive string matching
- Returns index of first matching column if duplicates exist
- The 1-based indexing convention matches PostgreSQL's internal attribute numbering system
- Simple utility function with no side effects or error handling requirements