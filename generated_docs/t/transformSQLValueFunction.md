# transformSQLValueFunction

## Location
[src/backend/parser/parse_expr.c:2302-2354](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2302-L2354)

## Overview
Transforms SQL value functions (like CURRENT_DATE, CURRENT_USER, etc.) by setting their appropriate result types and validating type modifiers where needed.

## Definition
```c
static Node *
transformSQLValueFunction(ParseState *pstate, SQLValueFunction *svf)
```

## Detailed Description
The `transformSQLValueFunction` function handles the semantic analysis of SQL value functions, which are special built-in functions that return system or session information without requiring explicit function call syntax. These functions include date/time functions (CURRENT_DATE, CURRENT_TIMESTAMP, etc.) and session information functions (CURRENT_USER, CURRENT_ROLE, etc.).

The function performs type assignment and validation through a comprehensive switch statement:

1. **Date/Time Functions**: Sets appropriate temporal types (DATE, TIME, TIMESTAMP variants) with timezone considerations
2. **Precision Validation**: For time/timestamp functions with precision parameters (e.g., CURRENT_TIME(3)), validates and normalizes the typmod using specialized validation functions
3. **Session Information Functions**: Sets NAME type for functions returning database/user names
4. **In-Place Modification**: Unlike other transformation functions, this modifies the original node rather than creating a new one

The function distinguishes between timezone-aware (CURRENT_TIME, CURRENT_TIMESTAMP) and local (LOCALTIME, LOCALTIMESTAMP) variants, setting the correct base type accordingly.

## Parameters / Member Variables
- `pstate`: Parse state containing context information for the current parsing operation (unused in this function)
- `svf`: The SQL value function node to be transformed, containing the operation type and optional typmod

## Dependencies
- Functions called/Symbols referenced:
  - [anytime_typmod_check](../a/anytime_typmod_check.md) (validates precision for TIME types)
  - [anytimestamp_typmod_check](../a/anytimestamp_typmod_check.md) (validates precision for TIMESTAMP types)
  - SVFOP_* constants (operation type identifiers for each SQL value function)
  - Type OIDs: DATEOID, TIMEOID, TIMETZOID, TIMESTAMPOID, TIMESTAMPTZOID, NAMEOID

- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)

## Notes and Other Information
- Unlike most transformation functions, this function modifies the input node in-place rather than creating a new node
- Time and timestamp functions support optional precision specifiers (0-6 digits for fractional seconds)
- The function handles both timezone-aware (with TZ suffix) and local variants of time functions
- [Session](../S/Session.md) information functions (CURRENT_USER, etc.) all return NAME type, which is a fixed-length string type in PostgreSQL
- Type modifier validation ensures precision values are within acceptable ranges for temporal types
- The function does not require access to the parse state, making it purely type-assignment focused
- CURRENT_CATALOG and CURRENT_SCHEMA return the current database and schema names respectively
- All supported functions are niladic (take no parameters) except for precision variants
- The transformation is deterministic based solely on the operation type stored in the node

## Simplified Source

```c
static Node *
transformSQLValueFunction(ParseState *pstate, SQLValueFunction *svf)
{
    // Set type and validate typmod based on SQL value function type
    switch (svf->op) {
        case SVFOP_CURRENT_DATE:
            svf->type = DATEOID;
            break;

        case SVFOP_CURRENT_TIME:
            svf->type = TIMETZOID;
            break;

        case SVFOP_CURRENT_TIME_N:
            svf->type = TIMETZOID;
            svf->typmod = anytime_typmod_check(true, svf->typmod);
            break;

        case SVFOP_CURRENT_TIMESTAMP:
            svf->type = TIMESTAMPTZOID;
            break;

        case SVFOP_CURRENT_TIMESTAMP_N:
            svf->type = TIMESTAMPTZOID;
            svf->typmod = anytimestamp_typmod_check(true, svf->typmod);
            break;

        case SVFOP_LOCALTIME:
            svf->type = TIMEOID;
            break;

        case SVFOP_LOCALTIME_N:
            svf->type = TIMEOID;
            svf->typmod = anytime_typmod_check(false, svf->typmod);
            break;

        case SVFOP_LOCALTIMESTAMP:
            svf->type = TIMESTAMPOID;
            break;

        case SVFOP_LOCALTIMESTAMP_N:
            svf->type = TIMESTAMPOID;
            svf->typmod = anytimestamp_typmod_check(false, svf->typmod);
            break;

        case SVFOP_CURRENT_ROLE:
        case SVFOP_CURRENT_USER:
        case SVFOP_USER:
        case SVFOP_SESSION_USER:
        case SVFOP_CURRENT_CATALOG:
        case SVFOP_CURRENT_SCHEMA:
            svf->type = NAMEOID;
            break;
    }

    return (Node *) svf;
}
```