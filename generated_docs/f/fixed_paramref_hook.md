# fixed_paramref_hook

## Location
[src/backend/parser/parse_param.c:99-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L99-L130)

## Overview
A callback function that transforms ParamRef nodes into Param nodes during query parsing when working with fixed parameters of predetermined types.

## Definition
```c
static Node *fixed_paramref_hook(ParseState *pstate, ParamRef *pref)
```

## Detailed Description
This hook function is called during query parsing to resolve parameter references ($1, $2, etc.) when the parameter types are known in advance. It validates that the parameter number is within the valid range and that the corresponding parameter type is valid, then creates a Param node with the appropriate type information. The function performs bounds checking and error reporting for invalid parameter references, ensuring that only properly defined parameters are used in the query.

## Parameters / Member Variables
- `pstate`: ParseState containing the parser context and hook state information
- `pref`: ParamRef node representing a parameter reference in the query ($n)

## Dependencies
- Functions called/Symbols referenced:
  - [FixedParamState](../F/FixedParamState.md) (structure type)
  - [ParamRef](../P/ParamRef.md) (node type)
  - [Param](../P/Param.md) (node type)
  - PARAM_EXTERN (parameter kind constant)
  - [get_typcollation](../g/get_typcollation.md) (function to get type collation)
  - makeNode (macro for creating nodes)
  - ereport/errcode/errmsg (error reporting)
  - OidIsValid (macro for OID validation)
- Called from (representative examples):
  - [setup_parse_fixed_parameters](../s/setup_parse_fixed_parameters.md) (installed as hook)

## Notes and Other Information
- This is a static function used exclusively as a callback hook
- Parameter numbers are 1-based, requiring adjustment for 0-based array access
- Validates parameter existence and type validity before creating Param nodes
- Sets paramtypmod to -1 (indicating no specific type modifier)
- Automatically determines parameter collation from the parameter type
- Error reporting includes parser position information for better user experience
- Part of PostgreSQL's prepared statement parameter handling infrastructure