# TypeCast

## Location
src/include/nodes/parsenodes.h: 370 - 376

## Overview
TypeCast represents a CAST expression in PostgreSQL's parse tree, used to explicitly convert one data type to another in SQL statements.

## Definition


## Detailed Description
TypeCast is a parse tree node that represents explicit type casting operations in SQL statements (e.g., CAST(expression AS type) or expression::type). It encapsulates both the source expression that needs to be converted and the target type specification. This node is created during parsing when the parser encounters type casting syntax and is later transformed during the analysis phase into appropriate coercion functions or operations.

## Parameters / Member Variables
- : NodeTag identifying this as a TypeCast node
- : Pointer to the Node representing the expression being cast
- : Pointer to TypeName specifying the target data type for the cast
- : ParseLoc storing the token's position in the source SQL, or -1 if location is unknown

## Dependencies
- Functions called/Symbols referenced:
  - TypeName
  - ParseLoc
  - Node (generic parse tree node)
- Called from (representative examples):
  - transformTypeCast
  - transformExprRecurse
  - FigureColnameInternal
  - transformColumnDefinition
  - raw_expression_tree_walker_impl

## Notes and Other Information
- TypeCast nodes are transformed into coercion functions during the analysis phase
- The location information helps provide accurate error messages for type conversion failures
- Both explicit CAST syntax and PostgreSQL's :: operator create TypeCast nodes
- The target TypeName may include type modifiers (e.g., VARCHAR(50))
- Used in column definitions, variable assignments, and general expression contexts