# variable_paramref_hook

## Location
src/backend/parser/parse_param.c: 131 - 185

## Overview
A callback function that transforms ParamRef nodes into Param nodes during query parsing for variable parameters, dynamically expanding the parameter type array as needed.

## Definition
```c
static Node *variable_paramref_hook(ParseState *pstate, ParamRef *pref)
```

## Detailed Description
This hook function handles parameter references ($1, $2, etc.) when parameter types are not predetermined and must be inferred during parsing. Unlike the fixed parameter version, this function dynamically expands the parameter type array to accommodate newly encountered parameters. It initializes new parameters with UNKNOWNOID type, which can later be refined through type coercion. The function includes special handling for JDBC compatibility, treating VOIDOID parameters in procedure calls as UNKNOWNOID to allow the JDBC driver to handle function and procedure calls uniformly.

## Parameters / Member Variables
- `pstate`: ParseState containing the parser context and hook state information
- `pref`: ParamRef node representing a parameter reference in the query ($n)

## Dependencies
- Functions called/Symbols referenced:
  - [VarParamState](../V/VarParamState.md) (structure type)
  - [ParamRef](../P/ParamRef.md) (node type)
  - Param (node type)
  - PARAM_EXTERN (parameter kind constant)
  - [get_typcollation](../g/get_typcollation.md) (function to get type collation)
  - makeNode (macro for creating nodes)
  - repalloc0_array (memory reallocation with zero-fill)
  - palloc0_array (memory allocation with zero-fill)
  - EXPR_KIND_CALL_ARGUMENT (expression kind constant)
  - UNKNOWNOID/VOIDOID/InvalidOid (type OID constants)
- Called from (representative examples):
  - [setup_parse_variable_parameters](../s/setup_parse_variable_parameters.md) (installed as hook)

## Notes and Other Information
- This is a static function used exclusively as a callback hook
- Dynamically grows the parameter type array using repalloc0_array when needed
- Initializes new parameters to UNKNOWNOID for later type inference
- Includes JDBC compatibility hack for void parameters in procedure calls
- Parameter numbers are 1-based, requiring adjustment for 0-based array access
- Sets paramtypmod to -1 (indicating no specific type modifier)
- Used primarily for ad-hoc queries where parameter types are determined contextually
- Memory is allocated in zero-filled blocks to ensure proper initialization