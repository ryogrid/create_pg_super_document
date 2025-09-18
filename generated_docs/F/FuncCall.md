# FuncCall

## Location
src/include/nodes/parsenodes.h: 423 - 437

## Overview
FuncCall represents a function or aggregate invocation in PostgreSQL's parse tree, supporting various function call syntaxes including regular functions, aggregates, window functions, and special constructs like DISTINCT, ORDER BY, and FILTER clauses.

## Definition


## Detailed Description
FuncCall is a comprehensive parse tree node that represents function and aggregate invocations in SQL statements. It supports a wide range of SQL function call syntax including regular function calls, aggregate functions with ORDER BY clauses, window functions with OVER clauses, DISTINCT aggregates, star aggregates (COUNT(*)), FILTER clauses, and WITHIN GROUP constructs. The node captures all the syntactic elements that can appear in function calls, making it suitable for representing everything from simple scalar functions to complex analytical functions.

## Parameters / Member Variables
- : NodeTag identifying this as a FuncCall node
- : List containing the qualified function name (schema.function_name)
- : List of expressions representing the function arguments
- : List of SortBy nodes for ORDER BY clauses in aggregates
- : Node representing a FILTER clause expression (for filtered aggregates)
- : Pointer to WindowDef structure for OVER clauses (window functions)
- : Boolean indicating ORDER BY appeared in WITHIN GROUP syntax
- : Boolean indicating the argument was '*' (as in COUNT(*))
- : Boolean indicating arguments were prefixed with DISTINCT
- : Boolean indicating the last argument was marked VARIADIC
- : CoercionForm enum controlling how the function call is displayed
- : ParseLoc storing the token's position in the source SQL

## Dependencies
- Functions called/Symbols referenced:
  - WindowDef
  - CoercionForm
  - ParseLoc
  - List (PostgreSQL list structure)
  - Node (generic parse tree node)
- Called from (representative examples):
  - makeFuncCall
  - transformFuncCall
  - transformExprRecurse
  - ParseFuncOrColumn
  - transformRangeFunction
  - FigureColnameInternal

## Notes and Other Information
- Typically initialized using makeFuncCall() helper function with sensible defaults
- Supports complex aggregate syntax including ORDER BY, FILTER, and WITHIN GROUP clauses
- Window function calls include OVER clause information for partitioning and ordering
- The funcformat field controls display format (explicit vs implicit function calls)
- Boolean flags help distinguish between different types of function calls during analysis
- Location information enables accurate error reporting for function call syntax errors
- Transformed during analysis phase into appropriate function call representations
- Essential for SQL standard compliance with advanced aggregate and window function features