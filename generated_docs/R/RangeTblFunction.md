# RangeTblFunction

## Location
[src/include/nodes/parsenodes.h:1317-1337](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1317-L1337)

## Overview
RangeTblFunction is subsidiary data for individual functions within a FUNCTION range table entry, storing function expressions and column definition information.

## Definition

```c
typedef struct RangeTblFunction
{
	NodeTag		type;

	Node	   *funcexpr;		/* expression tree for func call */
	/* number of columns it contributes to RTE */
	int			funccolcount pg_node_attr(query_jumble_ignore);
	/* These fields record the contents of a column definition list, if any: */
	/* column names (list of String) */
	List	   *funccolnames pg_node_attr(query_jumble_ignore);
	/* OID list of column type OIDs */
	List	   *funccoltypes pg_node_attr(query_jumble_ignore);
	/* integer list of column typmods */
	List	   *funccoltypmods pg_node_attr(query_jumble_ignore);
	/* OID list of column collation OIDs */
	List	   *funccolcollations pg_node_attr(query_jumble_ignore);

	/* This is set during planning for use by the executor: */
	/* PARAM_EXEC Param IDs affecting this func */
	Bitmapset  *funcparams pg_node_attr(query_jumble_ignore);
} RangeTblFunction;
```
## Detailed Description
RangeTblFunction represents individual functions within a FUNCTION range table entry. When a query contains function calls in the FROM clause, each function gets its own RangeTblFunction structure. This structure is particularly important for handling functions that return RECORD types with explicit column definition lists.

The structure stores the function expression tree and optional column definition information. When a function has an explicit column definition list (required for RECORD-returning functions), the column names, types, type modifiers, and collations are stored in the respective list fields. For functions returning named composite types, column information is not stored since it can change over time, but the column count is preserved to handle schema evolution gracefully.

During query planning, the funcparams bitmapset is populated to track PARAM_EXEC parameters that affect the function, enabling proper parameter handling during execution.

## Parameters / Member Variables
- : NodeTag identifying this as a RangeTblFunction node
- : Expression tree representing the function call
- : Number of columns this function contributes to the RTE
- : List of column names from explicit column definition list
- : List of OIDs representing column types from definition list
- : List of type modifiers for columns from definition list
- : List of OIDs representing column collations from definition list
- : Bitmapset of PARAM_EXEC parameter IDs affecting this function (set during planning)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - [Node](../N/Node.md)
  - [List](../L/List.md)
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [addRangeTableEntryForFunction](../a/addRangeTableEntryForFunction.md)
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md)
  - [ExecReScanFunctionScan](../E/ExecReScanFunctionScan.md)
  - [expandRTE](../e/expandRTE.md)
  - get_from_clause_item
  - [inline_set_returning_function](../i/inline_set_returning_function.md)
  - [set_function_size_estimates](../s/set_function_size_estimates.md)

## Notes and Other Information
- Only the funcexpr field is included in query jumbling for performance optimization
- Column definition information is stored only when explicitly provided (e.g., for RECORD functions)
- For named composite types, column information changes are handled by preserving column count
- The funcparams field is populated during planning phase for executor use
- Multiple RangeTblFunction entries can exist within a single FUNCTION RTE
- Handles both simple function calls and complex set-returning functions with column definitions
- Critical for proper execution of functions in FROM clauses and lateral joins