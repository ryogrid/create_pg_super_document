# op_error

## Location
[src/backend/parser/parse_oper.c:622-659](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L622-L659)

## Overview
The  function is a utility routine that generates appropriate error messages when an operator cannot be resolved during parsing.

## Definition

```c
struction.
 *
 * Transform operator expression ensuring type compatibility.
 * This is where some type conversion happens.
 *
 * last_srf should be a copy of pstate->p_last_srf from just before we
 * started transforming the operator's arguments;
```
## Detailed Description
This static function handles error reporting for unresolvable operators in PostgreSQL's parser. It analyzes the type of failure indicated by the  parameter and generates context-appropriate error messages. The function distinguishes between two main error scenarios: ambiguous operators (multiple matches found) and non-existent operators (no matches found).

For ambiguous operators, it suggests that explicit type casts might resolve the ambiguity. For non-existent operators, it provides different hints depending on whether it's a unary or binary operator, guiding users on how to resolve the issue through explicit type casting.

## Parameters / Member Variables
- : ParseState pointer for error context and location reporting
- : List containing the operator name that failed to resolve
- : OID of the first argument type (InvalidOid for unary operators)
- : OID of the second argument type (or only argument for unary operators)
- : FuncDetailCode indicating the type of resolution failure
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [op_signature_string](op_signature_string.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [errhint](../e/errhint.md)
  - [parser_errposition](../p/parser_errposition.md)
- Called from (representative examples):
  - [oper](oper.md)
  - [left_oper](../l/left_oper.md)

## Notes and Other Information
- Static function used internally within parse_oper.c
- Provides different error messages and hints based on the failure type
- Uses ERRCODE_AMBIGUOUS_FUNCTION for multiple matches
- Uses ERRCODE_UNDEFINED_FUNCTION for no matches
- Generates helpful hints about explicit type casting to resolve operator resolution issues
- The function never returns as it always raises an ERROR

## Simplified Source

```c
static void
op_error(ParseState *pstate, List *op,
         Oid arg1, Oid arg2,
         FuncDetailCode fdresult, int location)
{
    if (fdresult == FUNCDETAIL_MULTIPLE) {
        // Multiple operators found - ambiguous
        ereport(ERROR,
                (errcode(ERRCODE_AMBIGUOUS_FUNCTION),
                 errmsg("operator is not unique: %s",
                        op_signature_string(op, arg1, arg2)),
                 errhint("Could not choose a best candidate operator. "
                         "You might need to add explicit type casts."),
                 parser_errposition(pstate, location)));
    }
    else {
        // No operator found
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("operator does not exist: %s",
                        op_signature_string(op, arg1, arg2)),
                 (!arg1 || !arg2) ?
                 errhint("No operator matches the given name and argument type. "
                         "You might need to add an explicit type cast.") :
                 errhint("No operator matches the given name and argument types. "
                         "You might need to add explicit type casts."),
                 parser_errposition(pstate, location)));
    }
}
```