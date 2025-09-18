# BoolExprType

## Location
src/include/nodes/primnodes.h: 932 - 933

## Overview
BoolExprType is an enumeration that defines the types of basic Boolean operations (AND, OR, NOT) supported in PostgreSQL's expression system.

## Definition


## Detailed Description
BoolExprType specifies the three fundamental Boolean operations available in PostgreSQL's expression evaluation system. This enumeration is used within the BoolExpr node structure to indicate which Boolean operation should be performed on the provided arguments.

The enumeration supports:
- **AND_EXPR**: Logical AND operation that returns true only when all arguments are true
- **OR_EXPR**: Logical OR operation that returns true when at least one argument is true  
- **NOT_EXPR**: Logical NOT operation that negates a single argument

For AND and OR operations, the arguments are provided as a List that can contain two or more elements. For NOT operations, the list must contain exactly one element to be negated.

## Parameters / Member Variables
- : Represents logical AND operation (requires 2+ arguments)
- : Represents logical OR operation (requires 2+ arguments)  
- : Represents logical NOT operation (requires exactly 1 argument)

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this enum)
- Called from (representative examples):
  - BoolExpr struct (uses BoolExprType as boolop field)
  - [makeBoolExpr](../m/makeBoolExpr.md) function
  - [isSimpleNode](../i/isSimpleNode.md) function

## Notes and Other Information
- Used as the boolop field in BoolExpr structures to specify the Boolean operation type
- AND and OR expressions can handle multiple arguments efficiently through the List structure
- NOT expressions are restricted to single arguments for logical correctness
- Part of PostgreSQL's expression node system for representing Boolean logic in query trees
- Critical for query optimization and execution of Boolean predicates in WHERE clauses and conditional expressions