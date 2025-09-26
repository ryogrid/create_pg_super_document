# ExplainStmt

## Location
[src/include/nodes/parsenodes.h:3868-3873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3868-L3873)

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
  - [Node](../N/Node.md) (base type for query)
  - [List](../L/List.md) (for options storage)
  - [DefElem](../D/DefElem.md) (for option representation)
- Called from (representative examples):
  - [ExplainQuery](ExplainQuery.md)
  - [ExplainResultDesc](ExplainResultDesc.md)
  - [transformStmt](../t/transformStmt.md)
  - [transformExplainStmt](../t/transformExplainStmt.md)
  - [standard_ProcessUtility](../s/standard_ProcessUtility.md)
  - [UtilityTupleDescriptor](../U/UtilityTupleDescriptor.md)
  - [UtilityContainsQuery](../U/UtilityContainsQuery.md)
  - [GetCommandLogLevel](../G/GetCommandLogLevel.md)

## Notes and Other Information
- Part of PostgreSQL's utility statement framework
- The query planning and rewriting are deferred until execution to ensure EXPLAIN shows the actual plan that would be used
- Options are stored as DefElem nodes allowing for flexible parameter passing
- Used by both interactive EXPLAIN commands and internal query analysis tools
- Supports various output formats and analysis levels through the options mechanism