# regtypein

## Location
[src/backend/utils/adt/regproc.c:1176-1208](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regproc.c#L1176-L1208)

## Overview
Converts a string representation of a type name to its corresponding type OID, serving as the input function for the regtype data type.

## Definition

```c
Datum
regtypein(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the input function for the regtype data type that converts string representations of type names into their corresponding PostgreSQL type OIDs. The function supports multiple input formats and handles complex type syntax:

1. **Special values**: Accepts "-" to represent unknown type (OID 0)
2. **Numeric OID**: Accepts direct numeric OID values for symmetry with output functions
3. **Type names**: Handles simple type names like 'integer', 'text'
4. **Complex syntax**: Supports full PostgreSQL type syntax including:
   - Array types (e.g., 'INTEGER[]')
   - Qualified names (e.g., 'DOUBLE PRECISION')
   - Schema-qualified names (e.g., 'public.my_type')

The function uses PostgreSQL's full type parser to handle complex type specifications, ignoring any type modifier information that may be parsed but not relevant for type identification.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (char*): String representation of type name or OID to be converted
  -  (Node*): Error context for error reporting and handling

## Dependencies
- Functions called/Symbols referenced:
  - : Extract string argument from function call
  - : Parse special values ("-") and numeric OIDs
  - : Check if running in bootstrap mode
  - : Parse full type syntax and resolve to OID
  - : Return OID as PostgreSQL Datum
- Called from:
  - : Used in ACL (Access Control List) processing (src/backend/utils/adt/acl.c:4571)
  - : Helper function for type conversion (src/backend/utils/adt/regproc.c:1215)
  - : PL/Perl integration for literal conversion (src/pl/plperl/plperl.c:1454)

## Notes and Other Information
- Part of the regproc family of functions that handle object identifier conversions
- Bootstrap mode restrictions: Only accepts numeric OIDs during bootstrap, rejecting type names
- The function leverages PostgreSQL's full type parser, enabling support for complex type specifications
- Type modifier information from parsing is ignored since regtype only tracks type identity, not modifiers
- Input validation ensures that all non-special inputs must correspond to existing pg_type catalog entries
- Used internally by PostgreSQL when converting string literals to regtype values in SQL contexts

## Simplified Source

```c
Datum
regtypein(PG_FUNCTION_ARGS)
{
    char *typ_name_or_oid = PG_GETARG_CSTRING(0);
    Node *escontext = fcinfo->context;
    Oid result;
    int32 typmod;

    // Handle special "-" value or numeric OID
    if (parseDashOrOid(typ_name_or_oid, &result, escontext))
        PG_RETURN_OID(result);

    // Bootstrap mode restriction
    if (IsBootstrapProcessingMode())
        elog(ERROR, "regtype values must be OIDs in bootstrap mode");

    // Parse type name using full parser (handles arrays, qualified names, etc.)
    // Note: typmod is ignored for regtype purposes
    (void) parseTypeString(typ_name_or_oid, &result, &typmod, escontext);

    PG_RETURN_OID(result);
}
```