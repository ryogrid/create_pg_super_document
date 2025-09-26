# get_coercion_expr

## Location
[src/backend/utils/adt/ruleutils.c:11071-11134](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L11071-L11134)

## Overview
Generates string representation of value coercion expressions, formatting type casts using PostgreSQL's '::typename' notation while optimizing display for constants and handling pretty-printing preferences.

## Definition
```c
static void get_coercion_expr(Node *arg, deparse_context *context,
                              Oid resulttype, int32 resulttypmod,
                              Node *parentNode)
```

## Detailed Description
This function creates string representations of type coercion expressions, which convert values from one type to another. It implements intelligent optimization for constant values by checking if a Const node already matches the target type but has an unspecified typmod (-1). In such cases, it avoids redundant casting and displays the constant directly. For other expressions, it wraps the argument in parentheses (when pretty-printing is disabled) and appends the standard PostgreSQL cast notation '::typename'. The function specifically avoids using functional notation like 'typename(arg)' to prevent conflicts with user-defined functions and ensures consistent output format.

## Parameters / Member Variables
- `arg`: Pointer to the Node representing the value to be coerced
- `context`: Pointer to deparse_context containing deparsing state, buffer, and formatting options
- `resulttype`: OID of the target type for the coercion
- `resulttypmod`: Type modifier for the target type (length, precision, etc.)
- `parentNode`: Pointer to parent Node for context-sensitive parenthesization decisions

## Dependencies
- Functions called/Symbols referenced:
  - get_const_expr
  - get_rule_expr_paren
  - format_type_with_typemod
  - PRETTY_PAREN (formatting macro)
  - appendStringInfo functions
- Types referenced:
  - Node
  - deparse_context
  - Const
  - Oid, int32
- Called from (representative examples):
  - get_rule_expr
  - get_func_expr

## Notes and Other Information
The function includes sophisticated logic to avoid redundant type casting for constants that already have the correct type. It considers the interaction between parse_coerce.c's behavior and constant folding, ensuring that length coercion functions applied to constants are handled appropriately. The comment explains that collation information for constants would appear above the coercion node rather than below it, affecting how collation clauses are handled in the output. The function standardizes on PostgreSQL's '::' cast notation rather than SQL standard CAST() syntax for consistency with PostgreSQL conventions.