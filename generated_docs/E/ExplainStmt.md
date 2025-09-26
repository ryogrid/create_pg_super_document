# ExplainStmt

## Location
src/include/nodes/parsenodes.h: 3868 - 3873

## Overview
ExplainStmt represents the parsed structure of an EXPLAIN statement, which is used to display the execution plan and other information about how PostgreSQL would execute a query.

## Definition

```c
typedef struct ExplainStmt
{
	NodeTag		type;
	Node	   *query;			/* the query (see comments above) */
	List	   *options;		/* list of DefElem nodes */
} ExplainStmt;
```
## Detailed Description
ExplainStmt encapsulates an EXPLAIN command in PostgreSQL's parse tree. The query field initially contains a raw parse tree and is converted to a Query node during parse analysis. An important characteristic is that rewriting and planning of the query are always postponed until execution time. This allows EXPLAIN to show the actual execution plan that would be used. The options field contains various EXPLAIN modifiers like ANALYZE, VERBOSE, COSTS, etc.

## Parameters / Member Variables
- : NodeTag identifying this as an ExplainStmt node
- : Pointer to the query node being explained (initially raw parse tree, later converted to Query node)
- : List of DefElem nodes representing EXPLAIN options (ANALYZE, VERBOSE, COSTS, BUFFERS, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - Node (base type for query)
  - List (for options storage)
  - DefElem (for option representation)
- Called from (representative examples):
  - ExplainQuery
  - ExplainResultDesc
  - transformStmt
  - transformExplainStmt
  - standard_ProcessUtility
  - UtilityTupleDescriptor
  - UtilityContainsQuery
  - GetCommandLogLevel

## Notes and Other Information
- Part of PostgreSQL's utility statement framework
- The query planning and rewriting are deferred until execution to ensure EXPLAIN shows the actual plan that would be used
- Options are stored as DefElem nodes allowing for flexible parameter passing
- Used by both interactive EXPLAIN commands and internal query analysis tools
- Supports various output formats and analysis levels through the options mechanism