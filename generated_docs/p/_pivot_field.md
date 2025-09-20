# _pivot_field

## Location
[src/bin/psql/crosstabview.c:22-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/crosstabview.c#L22-L46)

## Overview
The _pivot_field structure represents a value/position from a PostgreSQL result set that forms part of the horizontal or vertical header in psql's crosstabview display format.

## Definition

```c
typedef struct _pivot_field
{
	/*
	 * Pointer obtained from PQgetvalue() for colV or colH. Each distinct
	 * value becomes an entry in the vertical header (colV), or horizontal
	 * header (colH). A Null value is represented by a NULL pointer.
	 */
	char	   *name;

	/*
	 * When a sort is requested on an alternative column, this holds
	 * PQgetvalue() for the sort column corresponding to <name>. If <name>
	 * appear multiple times, it's the first value in the order of the results
	 * that is kept. A Null value is represented by a NULL pointer.
	 */
	char	   *sort_value;

	/*
	 * Rank of this value, starting at 0. Initially, it's the relative
	 * position of the first appearance of <name> in the resultset. For
	 * example, if successive rows contain B,A,C,A,D then it's B:0,A:1,C:2,D:3
	 * When a sort column is specified, ranks get updated in a final pass to
	 * reflect the desired order.
	 */
	int			rank;
} pivot_field;
```
## Detailed Description
The _pivot_field structure is a core component of psql's crosstabview functionality, which transforms query results into a cross-tabulated (pivot table) format. Each instance represents a distinct value that appears in either the vertical header (colV) or horizontal header (colH) of the crosstab display. The structure supports both the display value and an optional sort value for ordering, along with ranking information to maintain proper positioning in the final output.

The structure is designed to handle NULL values gracefully by using NULL pointers for the name and sort_value fields. The ranking system initially reflects the order of first appearance in the result set, but can be updated when custom sorting is applied.

## Parameters / Member Variables
- `*name`: Pointer from PQgetvalue() representing the distinct value for this header entry; NULL represents SQL NULL values
- `*sort_value`: When alternative column sorting is requested, holds the PQgetvalue() result for the corresponding sort column; retains the first occurrence when name appears multiple times; NULL for SQL NULL values
- `rank`: Zero-based ranking of this value, initially set to the relative position of first appearance in the result set, updated during final sorting pass when sort column is specified
## Dependencies
- Functions called/Symbols referenced:
  - (None directly - this is a data structure definition)
- Called from (representative examples):
  - [_avl_node](../a/_avl_node.md) (as member field)
  - avl_tree (in tree operations)
  - PaintResultInCrosstab (for header management)
  - [printCrosstab](printCrosstab.md) (for display operations)
  - [avlInsertNode](../a/avlInsertNode.md) (for tree insertion)
  - [pivotFieldCompare](pivotFieldCompare.md) (for comparison operations)

## Notes and Other Information
- Part of psql's crosstabview feature located in src/bin/psql/crosstabview.c:22-46
- Used extensively in AVL tree operations for efficient pivot field management
- The typedef creates the alias 'pivot_field' for easier usage throughout the codebase
- Designed to work with PostgreSQL's libpq PQgetvalue() function results
- Supports both display and sorting scenarios with separate value storage