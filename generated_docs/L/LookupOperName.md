# LookupOperName

## Location
[src/backend/parser/parse_oper.c:99-132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_oper.c#L99-L132)

## Overview
Looks up an operator by name and exact input datatypes, with support for schema-qualified names and error handling options.

## Definition

```c
Oid
LookupOperName(ParseState *pstate, List *opername, Oid oprleft, Oid oprright,
			   bool noError, int location)
```
## Detailed Description
LookupOperName searches for an operator given its name (possibly schema-qualified) and the exact OIDs of its left and right operand types. The function uses the current namespace search path when the operator name is not schema-qualified. It provides flexible error handling - either returning InvalidOid or raising an error when the operator is not found. The function specifically checks for unsupported postfix operators and provides detailed error messages with position information.

## Parameters / Member Variables
- `*pstate`: Parse state for error reporting context (can be NULL)
- `*opername`: List containing the operator name (possibly schema-qualified)
- `oprleft`: OID of the left operand type (use InvalidOid for prefix operators)
- `oprright`: OID of the right operand type
- `noError`: If true, return InvalidOid on failure; if false, raise an error
- `location`: Token location for error reporting (use -1 if not available)
## Dependencies
- Functions called/Symbols referenced:
  - [OpernameGetOprid](../O/OpernameGetOprid.md)
  - [op_signature_string](../o/op_signature_string.md)
- Called from (representative examples):
  - [AggregateCreate](../A/AggregateCreate.md)
  - [OperatorLookup](../O/OperatorLookup.md)
  - [DefineOpClass](../D/DefineOpClass.md)
  - [LookupOperWithArgs](LookupOperWithArgs.md)

## Notes and Other Information
- Returns InvalidOid for failed lookups when noError is true
- Explicitly rejects postfix operators with a syntax error
- Uses exact type matching only - no type coercion is performed
- Error messages include the full operator signature for better diagnostics
- Part of the PostgreSQL parser's operator resolution system

## Simplified Source

```c
Oid
LookupOperName(ParseState *pstate, List *opername, Oid oprleft, Oid oprright,
               bool noError, int location)
{
    // Try to find the operator with exact type matching
    Oid result = OpernameGetOprid(opername, oprleft, oprright);
    if (OidIsValid(result))
        return result;

    // Handle not found case
    if (!noError) {
        // Check for unsupported postfix operators
        if (!OidIsValid(oprright))
            ereport(ERROR,
                    (errcode(ERRCODE_SYNTAX_ERROR),
                     errmsg("postfix operators are not supported"),
                     parser_errposition(pstate, location)));

        // Report operator not found error
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("operator does not exist: %s",
                        op_signature_string(opername, oprleft, oprright)),
                 parser_errposition(pstate, location)));
    }

    return InvalidOid;
}
```