# variable_paramref_hook

## Location
[src/backend/parser/parse_param.c:131-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_param.c#L131-L185)

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
  - [Param](../P/Param.md) (node type)
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

## Simplified Source

```c
static Node *
variable_paramref_hook(ParseState *pstate, ParamRef *pref)
{
    VarParamState *parstate = (VarParamState *) pstate->p_ref_hook_state;
    int paramno = pref->number;
    Oid *pptype;
    Param *param;

    // Check parameter number is valid
    if (paramno <= 0 || paramno > INT_MAX / sizeof(Oid))
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_PARAMETER),
             errmsg("there is no parameter $%d", paramno),
             parser_errposition(pstate, pref->location)));

    // Expand parameter array if necessary
    if (paramno > *parstate->numParams)
    {
        if (*parstate->paramTypes)
            *parstate->paramTypes = repalloc0_array(*parstate->paramTypes, Oid,
                                                    *parstate->numParams, paramno);
        else
            *parstate->paramTypes = palloc0_array(Oid, paramno);
        *parstate->numParams = paramno;
    }

    // Get pointer to this parameter's type slot
    pptype = &(*parstate->paramTypes)[paramno - 1];

    // Initialize to UNKNOWN if not seen before
    if (*pptype == InvalidOid)
        *pptype = UNKNOWNOID;

    // JDBC compatibility: treat void parameters in procedure calls as unknown
    if (*pptype == VOIDOID && pstate->p_expr_kind == EXPR_KIND_CALL_ARGUMENT)
        *pptype = UNKNOWNOID;

    // Create and return Param node
    param = makeNode(Param);
    param->paramkind = PARAM_EXTERN;
    param->paramid = paramno;
    param->paramtype = *pptype;
    param->paramtypmod = -1;
    param->paramcollid = get_typcollation(param->paramtype);
    param->location = pref->location;

    return (Node *) param;
}
```