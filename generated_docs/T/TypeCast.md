# TypeCast

## Location
[src/include/nodes/parsenodes.h:370-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L370-L376)

## Overview
TypeCast represents a CAST expression in PostgreSQL's parse tree, used to explicitly convert one data type to another in SQL statements.

## Definition

```c
typedef struct TypeCast
{
	NodeTag		type;
	Node	   *arg;			/* the expression being casted */
	TypeName   *typeName;		/* the target type */
	ParseLoc	location;		/* token location, or -1 if unknown */
} TypeCast;
```
## Detailed Description
TypeCast is a parse tree node that represents explicit type casting operations in SQL statements (e.g., CAST(expression AS type) or expression::type). It encapsulates both the source expression that needs to be converted and the target type specification. This node is created during parsing when the parser encounters type casting syntax and is later transformed during the analysis phase into appropriate coercion functions or operations.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a TypeCast node
- `*arg`: Pointer to the Node representing the expression being cast
- `*typeName`: Pointer to TypeName specifying the target data type for the cast
- `location`: ParseLoc storing the token's position in the source SQL, or -1 if location is unknown
## Dependencies
- Functions called/Symbols referenced:
  - [TypeName](TypeName.md)
  - ParseLoc
  - [Node](../N/Node.md) (generic parse tree node)
- Called from (representative examples):
  - [transformTypeCast](../t/transformTypeCast.md)
  - [transformExprRecurse](../t/transformExprRecurse.md)
  - [FigureColnameInternal](../F/FigureColnameInternal.md)
  - [transformColumnDefinition](../t/transformColumnDefinition.md)
  - [raw_expression_tree_walker_impl](../r/raw_expression_tree_walker_impl.md)

## Notes and Other Information
- [TypeCast](TypeCast.md) nodes are transformed into coercion functions during the analysis phase
- The location information helps provide accurate error messages for type conversion failures
- Both explicit CAST syntax and PostgreSQL's :: operator create TypeCast nodes
- The target TypeName may include type modifiers (e.g., VARCHAR(50))
- Used in column definitions, variable assignments, and general expression contexts