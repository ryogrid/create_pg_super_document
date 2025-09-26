# WindowFunc

## Location
src/include/nodes/primnodes.h: 563 - 588

## Overview
WindowFunc represents a window function expression node in PostgreSQL's query tree, used to store window functions that perform calculations across a set of table rows related to the current row.

## Definition

```c
typedef struct WindowFunc
{
	Expr		xpr;
	/* pg_proc Oid of the function */
	Oid			winfnoid;
	/* type Oid of result of the window function */
	Oid			wintype pg_node_attr(query_jumble_ignore);
	/* OID of collation of result */
	Oid			wincollid pg_node_attr(query_jumble_ignore);
	/* OID of collation that function should use */
	Oid			inputcollid pg_node_attr(query_jumble_ignore);
	/* arguments to the window function */
	List	   *args;
	/* FILTER expression, if any */
	Expr	   *aggfilter;
	/* List of WindowFuncRunConditions to help short-circuit execution */
	List	   *runCondition pg_node_attr(query_jumble_ignore);
	/* index of associated WindowClause */
	Index		winref;
	/* true if argument list was really '*' */
	bool		winstar pg_node_attr(query_jumble_ignore);
	/* is function a simple aggregate? */
	bool		winagg pg_node_attr(query_jumble_ignore);
	/* token location, or -1 if unknown */
	ParseLoc	location;
} WindowFunc;
```
## Detailed Description
WindowFunc is a specialized expression node that represents window functions in PostgreSQL's SQL execution. Window functions perform calculations across sets of rows that are related to the current row, without collapsing the result set like aggregate functions do. This structure stores all necessary information about the window function call, including the function identifier, type information, arguments, and execution optimization hints.

The struct includes several fields marked with  which indicates these fields are ignored during query plan hashing for plan cache purposes, as they represent internal execution state rather than semantic query content.

## Parameters / Member Variables
- : Base expression node structure (inherited from Expr)
- : OID of the window function from pg_proc catalog
- : Data type OID of the function's return value
- : Collation OID for the result value
- : Collation OID that the function should use for input processing
- : List of argument expressions passed to the window function
- : Optional FILTER clause expression for aggregate window functions
- : List of WindowFuncRunConditions for execution optimization and short-circuiting
- : Index reference to the associated WindowClause in the query
- : Boolean flag indicating if the argument list was specified as '*' (e.g., COUNT(*))
- : Boolean flag indicating if this is a simple aggregate function used as a window function
- : Parse location in the original query text for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ParseLoc (for location tracking)
  - Expr (base expression structure)
  - List (for args and runCondition)
  
- Called from (representative examples):
  - ExecInitWindowAgg (window aggregation execution initialization)
  - transformWindowFuncCall (parser transformation)
  - cost_windowagg (query planning cost estimation)
  - find_window_run_conditions (optimizer path planning)
  - get_windowfunc_expr (rule output formatting)

## Notes and Other Information
- Window functions are executed after the FROM, WHERE, GROUP BY, and HAVING clauses but before ORDER BY, LIMIT, and SELECT DISTINCT
- The runCondition field supports execution optimizations by allowing early termination when certain conditions are met
- Several fields are marked as query_jumble_ignore to ensure consistent plan caching across functionally equivalent queries
- Window functions can be either built-in window functions or aggregate functions used in a windowing context (indicated by winagg field)
- The structure supports both standard window functions and aggregate functions with FILTER clauses when used as window functions