# variable_coerce_param_hook

## Location
src/backend/parser/parse_param.c: 186 - 267

## Overview
A callback function that handles type coercion for variable parameters by updating their type information when the actual type is determined during query analysis.

## Definition
```c
static Node *variable_coerce_param_hook(ParseState *pstate, Param *param, Oid targetTypeId, int32 targetTypeMod, int location)
```

## Detailed Description
This hook function is called during query parsing to coerce variable parameters from UNKNOWNOID to their actual determined types. When a parameter's type is inferred from context (such as its usage in an expression or function call), this function updates both the local Param node and the shared parameter type array. It performs consistency checking to ensure that if a parameter is referenced multiple times, all references resolve to the same type. If conflicting types are detected, it reports an error with detailed type information to help users identify the problem.

## Parameters / Member Variables
- `pstate`: ParseState containing the parser context and hook state information
- `param`: Param node representing the parameter to be coerced
- `targetTypeId`: OID of the target type to coerce the parameter to
- `targetTypeMod`: Type modifier for the target type (not used, kept as -1)
- `location`: Source location for error reporting and position tracking

## Dependencies
- Functions called/Symbols referenced:
  - VarParamState (structure type)
  - Param (node type)
  - PARAM_EXTERN (parameter kind constant)
  - get_typcollation (function to get type collation)
  - format_type_be (function to format type names for error messages)
  - UNKNOWNOID (type OID constant)
  - ERRCODE_AMBIGUOUS_PARAMETER/ERRCODE_UNDEFINED_PARAMETER (error codes)
- Called from (representative examples):
  - setup_parse_variable_parameters (installed as hook)

## Notes and Other Information
- This is a static function used exclusively as a callback hook
- Only processes PARAM_EXTERN parameters with UNKNOWNOID type
- Returns NULL to signal normal coercion should proceed for other parameter types
- Maintains type consistency across multiple references to the same parameter
- Always sets paramtypmod to -1 to ensure runtime type checking occurs
- Uses default collation for the determined type rather than accepting custom collations
- Updates parameter location to the leftmost occurrence for better error reporting
- Critical for type inference in ad-hoc queries where parameter types are not predetermined
- Part of PostgreSQL's dynamic parameter type resolution system