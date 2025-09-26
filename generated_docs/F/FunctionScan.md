# FunctionScan

## Location
[src/include/nodes/plannodes.h:609-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/plannodes.h#L609-L614)

## Overview
FunctionScan represents a plan node for scanning the results of function calls in PostgreSQL's query execution tree, supporting table-valued functions and optionally providing row ordinality information.

## Definition

```c
typedef struct FunctionScan
{
	Scan		scan;
	List	   *functions;		/* list of RangeTblFunction nodes */
	bool		funcordinality; /* WITH ORDINALITY */
} FunctionScan;
```
## Detailed Description
FunctionScan is a plan node type that handles the execution of table-valued functions in SQL queries. It extends the base Scan node to provide specialized functionality for function scanning operations. The node can handle multiple functions simultaneously through its functions list and supports the SQL standard WITH ORDINALITY clause, which adds a sequential row number column to the output.

This node type is essential for queries that involve set-returning functions, table functions, or any scenario where a function call needs to be treated as a table source in the FROM clause. The executor uses this node to iterate through function results and present them as relational data.

## Parameters / Member Variables
- `scan`: Base Scan structure containing common scanning information like target lists and qualifications
- `*functions`: List of RangeTblFunction nodes representing the functions to be executed and scanned
- `funcordinality`: Boolean flag indicating whether to include row ordinality (sequential numbering) with WITH ORDINALITY clause
## Dependencies
- Functions called/Symbols referenced:
  - [Scan](../S/Scan.md) (base structure)
  - [List](../L/List.md) (for functions list)
  
- Called from (representative examples):
  - [ExecInitFunctionScan](../E/ExecInitFunctionScan.md) (executor initialization)
  - [ExecReScanFunctionScan](../E/ExecReScanFunctionScan.md) (executor rescanning)
  - [create_functionscan_plan](../c/create_functionscan_plan.md) (plan creation)
  - [make_functionscan](../m/make_functionscan.md) (plan node construction)
  - [set_plan_refs](../s/set_plan_refs.md) (plan reference setting)

## Notes and Other Information
- Part of PostgreSQL's plan node hierarchy for representing different types of scan operations
- Supports complex function scanning scenarios including multiple functions and ordinality
- Integrated with the executor framework for efficient function result processing
- Used in conjunction with RangeTblFunction nodes to represent function calls in range tables
- Essential for implementing SQL standard table function capabilities