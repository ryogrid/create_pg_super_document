# ReturnStmt

## Location
[src/include/nodes/parsenodes.h:2210-2214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2210-L2214)

## Overview
ReturnStmt represents a RETURN statement inside a SQL function body, used to return a value from a function.

## Definition

```c
typedef struct ReturnStmt
{
	NodeTag		type;
	Node	   *returnval;
} ReturnStmt;
```
## Detailed Description
ReturnStmt is a parse node structure that represents a RETURN statement within SQL function bodies. When a SQL function contains a RETURN statement, the parser creates this node to hold the return value expression. The structure is designed to be part of PostgreSQL's unified node system and contains the expression to be returned from the function.

During query transformation, ReturnStmt nodes are processed by transformReturnStmt() which converts them into Query nodes with commandType CMD_SELECT and isReturn flag set to true. This allows the return statement to be executed as a special form of SELECT query.

## Parameters / Member Variables
- : NodeTag identifying this as a ReturnStmt node
- : Node pointer to the expression that should be returned (can be NULL for functions returning void)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (inherited node type system)
  - Node (base node type)
- Called from (representative examples):
  - transformStmt (general statement transformation)
  - transformReturnStmt (specific return statement transformation)

## Notes and Other Information
- Only valid within SQL function bodies, not in regular SQL statements
- The returnval expression is transformed and type-checked during query analysis
- Converted to a Query node with isReturn=true during transformation phase
- Part of PostgreSQL's extensible node system for representing parse trees