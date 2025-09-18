# paramlist_param_ref

## Location
src/backend/nodes/params.c: 131 - 166

## Overview
Transforms a ParamRef node into a Param node during parsing, resolving parameter type information from a ParamListInfo structure.

## Definition


## Detailed Description
The paramlist_param_ref function is a parameter reference hook that gets called during query parsing when a parameter reference (, , etc.) is encountered. It transforms a ParamRef parse node into a Param execution node by looking up parameter type information from the ParamListInfo structure stored in the ParseState. The function validates the parameter number, retrieves parameter information (potentially through a paramFetch hook for dynamic parameters), and creates a properly typed Param node with the correct type, collation, and location information. If the parameter is invalid or has no valid type, it returns NULL.

## Parameters / Member Variables
- : The current ParseState containing the ParamListInfo in p_ref_hook_state
- : The ParamRef node representing the parameter reference () to be transformed

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates a new Param node)
  - [get_typcollation](../g/get_typcollation.md) (gets the collation for the parameter type)
  - OidIsValid (macro to validate OID)
  - [ParamListInfo](../P/ParamListInfo.md) (parameter list structure type)
  - ParamExternData (individual parameter data type)
  - Param (execution node for parameters)
  - PARAM_EXTERN (parameter kind constant)
- Called from (representative examples):
  - [paramlist_parser_setup](paramlist_parser_setup.md) (sets this as the p_paramref_hook)

## Notes and Other Information
- This is a static function, only accessible within params.c
- Returns NULL if the parameter number is invalid (≤ 0 or > numParams)
- Returns NULL if the parameter has no valid type (OidIsValid check fails)
- Handles both static and dynamic parameters through the paramFetch hook mechanism
- Sets paramtypmod to -1 (default type modifier)
- Uses 1-based parameter numbering (, , etc.) but converts to 0-based array indexing
- The function is located in src/backend/nodes/params.c at lines 131-166