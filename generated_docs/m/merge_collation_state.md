# merge_collation_state

## Location
[src/backend/parser/parse_collate.c:780-880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_collate.c#L780-L880)

## Overview
Merges collation state from a subexpression into the parent context, resolving conflicts and determining precedence between different collation strengths.

## Definition

```c
static void
merge_collation_state(Oid collation,
					  CollateStrength strength,
					  int location,
					  Oid collation2,
					  int location2,
					  assign_collations_context *context)
```
## Detailed Description
This function implements the core logic for combining collation information from child expressions with the parent's collation context. It follows PostgreSQL's collation precedence rules:

1. **Strength Hierarchy**: Explicit collations (COLLATE clauses) take precedence over implicit collations, which take precedence over no collation
2. **Conflict Resolution**: When implicit collations conflict, non-default collations beat default collations
3. **Error Handling**: Explicit collation conflicts cause immediate errors, while implicit conflicts are deferred for potential later resolution

The function updates the parent context with the stronger collation state, or marks conflicts that will be reported later if they remain unresolved.

## Parameters / Member Variables
- : The collation OID from the current subexpression
- : The collation strength (COLLATE_NONE, COLLATE_IMPLICIT, COLLATE_EXPLICIT, or COLLATE_CONFLICT)
- : Source location of the current collation for error reporting
- : Secondary collation OID (used for conflict reporting)
- : Location of secondary collation (used for conflict reporting)
- : Parent collation context to be updated with merged state

## Dependencies
- Functions called/Symbols referenced:
  -  (for error message formatting)
  - , , ,  (error reporting)
  -  (default collation constant)
- Called from (representative examples):
  -  (main recursive collation assignment)
  -  (aggregate function collation handling)

## Notes and Other Information
- The function follows SQL standard behavior for explicit collation conflicts (immediate error)
- Implicit collation conflicts are deferred to allow potential resolution by later COLLATE clauses
- Non-default implicit collations take precedence over default collations to provide more specific behavior
- The conflict information (collation2, location2) is preserved to generate meaningful error messages if conflicts cannot be resolved

## Simplified Source

```c
static void
merge_collation_state(Oid collation,
                      CollateStrength strength,
                      int location,
                      Oid collation2,
                      int location2,
                      assign_collations_context *context)
{
    // Higher strength always dominates
    if (strength > context->strength)
    {
        // Override previous parent state
        context->collation = collation;
        context->strength = strength;
        context->location = location;

        // Preserve conflict info if applicable
        if (strength == COLLATE_CONFLICT)
        {
            context->collation2 = collation2;
            context->location2 = location2;
        }
    }
    else if (strength == context->strength)
    {
        // Same strength - merge or detect conflicts
        switch (strength)
        {
            case COLLATE_NONE:
                // Nothing + nothing = nothing
                break;

            case COLLATE_IMPLICIT:
                if (collation != context->collation)
                {
                    // Non-default beats default
                    if (context->collation == DEFAULT_COLLATION_OID)
                    {
                        context->collation = collation;
                        context->location = location;
                    }
                    else if (collation != DEFAULT_COLLATION_OID)
                    {
                        // Conflict detected - defer error
                        context->strength = COLLATE_CONFLICT;
                        context->collation2 = collation;
                        context->location2 = location;
                    }
                }
                break;

            case COLLATE_EXPLICIT:
                if (collation != context->collation)
                {
                    // Immediate error for explicit conflicts
                    ereport(ERROR,
                            (errcode(ERRCODE_COLLATION_MISMATCH),
                             errmsg("collation mismatch between explicit collations \"%s\" and \"%s\"",
                                    get_collation_name(context->collation),
                                    get_collation_name(collation)),
                             parser_errposition(context->pstate, location)));
                }
                break;

            case COLLATE_CONFLICT:
                // Still conflicted
                break;
        }
    }
}
```