# FunctionParameter

## Location
[src/include/nodes/parsenodes.h:3451-3458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3451-L3458)

## Overview
FunctionParameter represents a single parameter definition within a CREATE FUNCTION or CREATE PROCEDURE statement, encapsulating the parameter's name, type, mode, and default value.

## Definition

```c
typedef struct FunctionParameter
{
	NodeTag		type;
	char	   *name;			/* parameter name, or NULL if not given */
	TypeName   *argType;		/* TypeName for parameter type */
	FunctionParameterMode mode; /* IN/OUT/etc */
	Node	   *defexpr;		/* raw default expr, or NULL if not given */
} FunctionParameter;
```
## Detailed Description
FunctionParameter is a parse tree node that represents individual parameters in function and procedure definitions. It captures all aspects of a parameter specification including its name, data type, parameter mode (IN, OUT, INOUT, VARIADIC), and optional default value. This structure supports PostgreSQL's comprehensive parameter system for functions and procedures.

The parameter mode determines how the parameter behaves: IN parameters are input-only, OUT parameters are output-only, INOUT parameters serve both purposes, and VARIADIC parameters accept a variable number of arguments. Default expressions allow parameters to be optional in function calls.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL parse tree nodes  
- : Name of the parameter (can be NULL for unnamed parameters)
- : TypeName node specifying the parameter's data type
- : FunctionParameterMode enum value (IN, OUT, INOUT, VARIADIC, or TABLE)
- : Parse tree node containing the default value expression (NULL if no default)

## Dependencies
- Functions called/Symbols referenced:
  - TypeName (for parameter type specification)
  - FunctionParameterMode (parameter mode enumeration)
  - NodeTag (parse tree infrastructure)
  - Node (base parse tree node type)

- Called from (representative examples):
  - interpret_function_parameter_list (function signature processing)
  - LookupFuncWithArgs (function resolution by signature)
  - exprLocation (parse tree location tracking)

## Notes and Other Information
- Used within the parameters list of CreateFunctionStmt structures
- Parameter names are optional - PostgreSQL supports positional parameter calling
- Default expressions are evaluated at function call time, not definition time
- VARIADIC parameters must be the last parameter and accept array-like input
- OUT and INOUT parameters affect the function's effective return type
- TABLE mode parameters are used in table functions for column definitions
- Part of PostgreSQL's comprehensive function parameter system supporting modern SQL standards