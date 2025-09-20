# sql_fn_param_ref

## Location
[src/backend/executor/functions.c:394-409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L394-L409)

## Overview
Parser callback function for handling ParamRef nodes ( symbols) in SQL function bodies.

## Definition

```c
struct a Param node for the given paramno
 */
static Node *
sql_fn_make_param(SQLFunctionParseInfoPtr pinfo,
				  int paramno, int location)
{
	Param	   *param;

	param = makeNode(Param);
	param->paramkind = PARAM_EXTERN;
	param->paramid = paramno;
	param->paramtype = pinfo->argtypes[paramno - 1];
	param->paramtypmod = -1;
	param->paramcollid = get_typcollation(param->paramtype);
	param->location = location;

	/*
	 * If we have a function input collation, allow it to override the
	 * type-derived collation for parameter symbols.  (XXX perhaps this should
	 * not happen if the type collation is not default?)
	 */
	if (OidIsValid(pinfo->collation) && OidIsValid(param->paramcollid))
		param->paramcollid = pinfo->collation;

	return (Node *) param;
}

/*
 * Search for a function parameter of the given name;
```
## Detailed Description
This function serves as a callback for processing parameter references (, , etc.) encountered during SQL function parsing. It validates that the parameter number is within the valid range for the function's declared parameters and delegates the actual parameter node creation to sql_fn_make_param. This function ensures that only valid parameter numbers are processed and provides proper error handling for out-of-range parameter references.

## Parameters / Member Variables
- : ParseState containing parser context and hook state information
- : ParamRef node representing the parameter reference () to be processed

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