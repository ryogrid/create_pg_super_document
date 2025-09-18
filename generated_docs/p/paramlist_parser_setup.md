# paramlist_parser_setup

## Location
[src/backend/nodes/params.c:120-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/params.c#L120-L130)

## Overview
Sets up parser hooks to handle parameter references during query parsing when parameters are sourced from a ParamListInfo structure.

## Definition


## Detailed Description
The paramlist_parser_setup function is a parser setup callback that configures the PostgreSQL parser to handle parameter references (, , etc.) by setting up the appropriate hooks in the ParseState structure. It specifically sets the p_paramref_hook to paramlist_param_ref, which will be called whenever the parser encounters a parameter reference. The function does not set up a parameter coercion hook (p_coerce_param_hook) as indicated by the comment. The arg parameter, which should be a ParamListInfo structure, is stored in p_ref_hook_state for use by the parameter reference hook.

## Parameters / Member Variables
- : Pointer to the ParseState structure that needs to be configured with parameter handling hooks
- : A void pointer that should contain the ParamListInfo structure to be used for parameter resolution

## Dependencies
- Functions called/Symbols referenced:
  - [paramlist_param_ref](paramlist_param_ref.md) (the parameter reference hook function)
- Called from (representative examples):
  - [makeParamList](../m/makeParamList.md) (as the default parser setup function)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the params.c file
- The function deliberately does not use the p_coerce_param_hook as parameter coercion is not needed for this use case
- The arg parameter is typically a ParamListInfo structure that will be used by paramlist_param_ref to resolve parameter values
- This function is automatically set as the default parserSetup in makeParamList
- The function is located in src/backend/nodes/params.c at lines 120-130