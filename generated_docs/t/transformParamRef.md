# transformParamRef

## Location
src/backend/parser/parse_expr.c: 886 - 909

## Overview
Transforms a parameter reference (, , etc.) from parse tree representation into an executable expression node, utilizing a pluggable hook mechanism for parameter resolution.

## Definition


## Detailed Description
The  function is responsible for transforming parameter references (like , , etc.) encountered during SQL parsing. The core parser itself doesn't have built-in knowledge about parameter handling, so it delegates this responsibility to a configurable hook mechanism.

The function first checks if a parameter reference hook () is registered in the parse state. If such a hook exists, it calls the hook function to perform the actual parameter transformation. If no hook is registered or if the hook returns NULL (indicating it couldn't handle the parameter), the function raises an error indicating that the specified parameter number doesn't exist.

This design allows different parts of PostgreSQL (such as prepared statements, PL/pgSQL, etc.) to register their own parameter handling logic while keeping the core parser generic.

## Parameters / Member Variables
- : Parse state containing context information including the parameter reference hook
- : The parameter reference node containing the parameter number and source location

## Dependencies
- Functions called/Symbols referenced:
  - [ParamRef](../P/ParamRef.md) (struct type for parameter references)
  - ereport (error reporting function)
  - [parser_errposition](../p/parser_errposition.md) (for error location reporting)
- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md)

## Notes and Other Information
- This function is static and only used within the parse_expr.c module
- The hook-based design provides flexibility for different parameter handling strategies
- Parameter numbering starts from 1 (e.g., , , ...)
- Error messages include the specific parameter number and source location for better diagnostics
- The function is part of PostgreSQL's expression transformation pipeline during query parsing