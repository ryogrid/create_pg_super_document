# LookupCollation

## Location
[src/backend/parser/parse_type.c:515-539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_type.c#L515-L539)

## Overview
LookupCollation is a function that looks up a collation by name and returns its OID, with support for error location reporting during parsing.

## Definition
```c
Oid LookupCollation(ParseState *pstate, List *collnames, int location)
```

## Detailed Description
This function serves as a wrapper around get_collation_oid() that adds parser error location tracking. It takes a list of collation names (which may be schema-qualified) and resolves them to a collation OID. The function sets up error position callbacks to provide accurate error reporting when collation lookup fails during SQL parsing.

The function conditionally sets up parser error callbacks only when a ParseState is provided, making it usable in contexts both with and without full parser state tracking.

## Parameters / Member Variables
- `pstate`: ParseState pointer for error location tracking; can be NULL if error position tracking is not needed
- `collnames`: List of strings representing the collation name (possibly schema-qualified)
- `location`: Character position in the source query for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [ParseCallbackState](../P/ParseCallbackState.md)
  - [setup_parser_errposition_callback](../s/setup_parser_errposition_callback.md)
  - [get_collation_oid](../g/get_collation_oid.md)
  - [cancel_parser_errposition_callback](../c/cancel_parser_errposition_callback.md)
- Called from (representative examples):
  - [resolve_unique_index_expr](../r/resolve_unique_index_expr.md)
  - [transformCollateClause](../t/transformCollateClause.md)
  - [GetColumnDefCollation](../G/GetColumnDefCollation.md)
  - [transformColumnType](../t/transformColumnType.md)

## Notes and Other Information
- The function provides a thin wrapper around get_collation_oid() with added error position tracking
- Error callbacks are only set up when pstate is non-NULL, allowing the function to work in various parsing contexts
- Returns InvalidOid if the collation cannot be found (behavior inherited from get_collation_oid)
- Located in src/backend/parser/parse_type.c:515-539

## Simplified Source

```c
Oid LookupCollation(ParseState *pstate, List *collnames, int location) {
    Oid colloid;
    ParseCallbackState pcbstate;

    // Set up error position tracking if parser state is available
    if (pstate)
        setup_parser_errposition_callback(&pcbstate, pstate, location);

    // Look up the collation by name
    colloid = get_collation_oid(collnames, false);

    // Clean up error callback
    if (pstate)
        cancel_parser_errposition_callback(&pcbstate);

    return colloid;
}
```