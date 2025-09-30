# select_common_collation

## Location
[src/backend/parser/parse_collate.c:191-241](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L191-L241)

## Overview
Identifies a common collation for a list of expressions, following SQL standard rules for collation determination with optional conflict handling.

## Definition

```c
Oid
select_common_collation(ParseState *pstate, List *exprs, bool none_ok)
```

## Simplified Source

```c
Oid
select_common_collation(ParseState *pstate, List *exprs, bool none_ok)
{
    assign_collations_context context;

    // Initialize context for tree walk
    context.pstate = pstate;
    context.collation = InvalidOid;
    context.strength = COLLATE_NONE;
    context.location = -1;

    // Process all expressions to determine common collation
    assign_collations_walker((Node *) exprs, &context);

    // Handle collation conflicts
    if (context.strength == COLLATE_CONFLICT)
    {
        if (none_ok)
            return InvalidOid;

        // Report conflict error
        ereport(ERROR,
                (errcode(ERRCODE_COLLATION_MISMATCH),
                 errmsg("collation mismatch between implicit collations \"%s\" and \"%s\"",
                        get_collation_name(context.collation),
                        get_collation_name(context.collation2)),
                 errhint("You can choose the collation by applying the COLLATE clause to one or both expressions."),
                 parser_errposition(context.pstate, context.location2)));
    }

    // Return determined collation (InvalidOid if no collatable types)
    return context.collation;
}
```