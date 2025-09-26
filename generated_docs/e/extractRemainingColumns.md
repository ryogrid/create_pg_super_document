# extractRemainingColumns

## Location
[src/backend/parser/parse_clause.c:255-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L255-L307)

## Overview
Extracts all columns that are not already merged from JOIN USING clauses, efficiently handling the remaining columns of a source table for JOIN operations.

## Definition

```c
static int
extractRemainingColumns(ParseState *pstate,
						ParseNamespaceColumn *src_nscolumns,
						List *src_colnames,
						List **src_colnos,
						List **res_colnames, List **res_colvars,
						ParseNamespaceColumn *res_nscolumns)
```
## Detailed Description
The `extractRemainingColumns` function is a static helper function used during JOIN processing to efficiently identify and extract columns from a source table that have not already been merged through JOIN USING clauses. This is crucial for constructing the complete column list of a JOIN result.

The function operates in two phases:
1. **Optimization phase**: Creates a bitmapset of already-merged column numbers to avoid O(N²) complexity when checking for duplicate columns
2. **Extraction phase**: Iterates through all source table columns, identifying non-dropped columns that haven't been merged, and adds them to the result structures

For each remaining column, the function:
- Adds its column number to the source column number list
- Appends its name to the result column names list
- Creates a Var node for the column and adds it to the result column variables list
- Copies the namespace column metadata to the result namespace columns array

The function is designed to handle wide tables efficiently by using bitmapset operations rather than repeated list membership checks.

## Parameters / Member Variables
- `pstate`: The current parse state containing parsing context information
- `src_nscolumns`: Array of ParseNamespaceColumn structures describing the source table's columns
- `src_colnames`: List of column names from the source table
- `src_colnos`: Pointer to list of column numbers for already-merged columns (modified by function)
- `res_colnames`: Pointer to result list of column names (modified by function)
- `res_colvars`: Pointer to result list of column Var nodes (modified by function)
- `res_nscolumns`: Caller-allocated array to store namespace column data for result columns

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceColumn](../P/ParseNamespaceColumn.md)
  - [bms_add_member](../b/bms_add_member.md)
  - lfirst_int
  - [bms_is_member](../b/bms_is_member.md)
  - [lappend_int](../l/lappend_int.md)
  - [buildVarFromNSColumn](../b/buildVarFromNSColumn.md)
- Called from (representative examples):
  - [transformFromClauseItem](../t/transformFromClauseItem.md) (multiple locations)

## Notes and Other Information
- This is a static (internal) function within parse_clause.c, not exposed in the public API
- Uses bitmapset optimization to avoid O(N²) complexity for wide tables with many columns
- Handles dropped columns by checking for empty column names (colname[0] != '\0')
- The caller is responsible for allocating sufficient space in res_nscolumns array
- Essential for JOIN USING clause processing where some columns are merged and others need to be preserved individually
- Returns the count of columns added to the result structures