# AlterOperatorStmt

## Location
[src/include/nodes/parsenodes.h:3584-3589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3584-L3589)

## Overview
AlterOperatorStmt is a PostgreSQL parse node structure that represents an ALTER OPERATOR SET statement for modifying operator properties and attributes.

## Definition

```c
typedef struct AlterOperatorStmt
{
	NodeTag		type;
	ObjectWithArgs *opername;	/* operator name and argument types */
	List	   *options;		/* List of DefElem nodes */
} AlterOperatorStmt;
```
## Detailed Description
AlterOperatorStmt represents SQL statements that modify properties of existing operators in PostgreSQL. This allows database administrators and extension developers to alter operator characteristics such as cost estimates, selectivity estimates, and other operator-specific properties that affect query planning and execution. The structure uses ObjectWithArgs to precisely identify the operator by name and argument types, and a list of DefElem nodes to specify the properties being modified.

## Parameters / Member Variables
- : Standard NodeTag for parse tree identification
- : ObjectWithArgs pointer containing the operator name and its argument types for precise identification
- : List of DefElem nodes specifying the operator properties to be modified

## Dependencies
- Functions called/Symbols referenced:
  - [ObjectWithArgs](../O/ObjectWithArgs.md) (structure for objects with argument specifications)
  - [List](../L/List.md) (PostgreSQL generic list type)
  - [DefElem](../D/DefElem.md) (definition element for specifying options)
  - NodeTag (standard parse node identification)
- Called from (representative examples):
  - [AlterOperator](AlterOperator.md) (main execution function for operator alteration)
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md) (utility command processing)

## Notes and Other Information
Operator modification is a relatively specialized operation in PostgreSQL, typically used by extension developers and database experts who need to fine-tune operator behavior for performance optimization. The ObjectWithArgs structure ensures that operators are uniquely identified even when multiple operators share the same name but have different argument types (operator overloading). The options list can contain various operator properties such as RESTRICT, JOIN, HASHES, MERGES, and cost-related parameters that influence the query optimizer's decisions.