# sql_fn_param_ref

## Location
[src/backend/executor/functions.c:394-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L394-L409)

## Overview
Parser callback function for handling ParamRef nodes ( symbols) in SQL function bodies.

## Definition

```c
static Node *
sql_fn_param_ref(ParseState *pstate, ParamRef *pref)
```
## Detailed Description
This function serves as a callback for processing parameter references (, , etc.) encountered during SQL function parsing. It validates that the parameter number is within the valid range for the function's declared parameters and delegates the actual parameter node creation to sql_fn_make_param. This function ensures that only valid parameter numbers are processed and provides proper error handling for out-of-range parameter references.

## Parameters / Member Variables
- `*pstate`: ParseState containing parser context and hook state information
- `*pref`: ParamRef node representing the parameter reference () to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [ParamRef](../P/ParamRef.md)
  - SQLFunctionParseInfoPtr
  - [sql_fn_make_param](sql_fn_make_param.md)
- Called from (representative examples):
  - [sql_fn_parser_setup](sql_fn_parser_setup.md) (src/backend/executor/functions.c:269)

## Notes and Other Information
- Validates parameter numbers are positive and within the function's argument count (1 to nargs)
- Returns NULL for invalid parameter numbers, allowing the parser to handle the error appropriately
- Acts as a thin validation wrapper around sql_fn_make_param for parameter reference processing
- Parameter numbering follows PostgreSQL's 1-based indexing convention for function parameters
- The location information from the ParamRef is passed through to sql_fn_make_param for error reporting

## Simplified Source

```c
static Node *sql_fn_param_ref(ParseState *pstate, ParamRef *pref) {
    SQLFunctionParseInfoPtr pinfo = (SQLFunctionParseInfoPtr) pstate->p_ref_hook_state;
    int paramno = pref->number;

    // Validate parameter number
    if (paramno <= 0 || paramno > pinfo->nargs)
        return NULL;  // Invalid parameter number

    // Create and return parameter node
    return sql_fn_make_param(pinfo, paramno, pref->location);
}
```