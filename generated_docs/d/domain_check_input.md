# domain_check_input

## Location
[src/backend/utils/adt/domains.c:138-226](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/domains.c#L138-L226)

## Overview
Applies cached domain constraints to validate input values, executing NOT NULL and CHECK constraints separately.

## Definition

```c
static void
domain_check_input(Datum value, bool isnull, DomainIOData *my_extra,
				   Node *escontext)
```
## Detailed Description
The  function validates a value against all domain constraints defined for a domain type. This function is roughly similar to the handling of CoerceToDomain nodes in execExpr*.c, but executes each constraint separately rather than compiling them in-line within a larger expression.

The function processes two types of domain constraints:
1. **NOT NULL constraints**: Checks if the value is null when the domain doesn't allow null values
2. **CHECK constraints**: Evaluates custom CHECK expressions against the input value

If the  parameter points to an ErrorSaveContext, any failures are reported there; otherwise they are reported via ereport. The function does not attempt soft reporting of errors raised during execution of CHECK constraints.

## Parameters / Member Variables
- `value`: The Datum value to be validated against domain constraints
- `isnull`: Boolean flag indicating whether the value is null
- `*my_extra`: Pointer to DomainIOData structure containing cached constraint information
- `*escontext`: Node for error context handling (can be ErrorSaveContext or NULL)
## Dependencies
- Functions called/Symbols referenced:
  - [DomainIOData](../D/DomainIOData.md) (struct type)
  - [UpdateDomainConstraintRef](../U/UpdateDomainConstraintRef.md)
  - [DomainConstraintState](../D/DomainConstraintState.md)
  - DOM_CONSTRAINT_NOTNULL
  - errsave
  - [errdatatype](../e/errdatatype.md)
  - DOM_CONSTRAINT_CHECK
  - [CreateStandaloneExprContext](../C/CreateStandaloneExprContext.md)
  - MakeExpandedObjectReadOnly
  - [ExecCheck](../E/ExecCheck.md)
  - [errdomainconstraint](../e/errdomainconstraint.md)
  - [ReScanExprContext](../R/ReScanExprContext.md)

- Called from (representative examples):
  - [domain_in](domain_in.md) (src/backend/utils/adt/domains.c:275)
  - [domain_recv](domain_recv.md) (src/backend/utils/adt/domains.c:331)
  - [domain_check_internal](domain_check_internal.md) (src/backend/utils/adt/domains.c:397)

## Notes and Other Information
- The function creates an ExprContext lazily when needed for CHECK constraint evaluation
- Values are protected against modification during constraint checking using MakeExpandedObjectReadOnly
- The function performs cleanup by calling ReScanExprContext to avoid leaking non-memory resources
- [Constraint](../C/Constraint.md) validation follows a fail-fast approach - the first failed constraint causes the function to jump to cleanup
- The function updates domain constraints before validation to ensure they are current

## Simplified Source

```c
static void domain_check_input(Datum value, bool isnull, DomainIOData *my_extra,
                              Node *escontext) {
    ExprContext *econtext = my_extra->econtext;
    ListCell *l;

    // Ensure constraints are up-to-date
    UpdateDomainConstraintRef(&my_extra->constraint_ref);

    // Check each constraint
    foreach(l, my_extra->constraint_ref.constraints) {
        DomainConstraintState *con = (DomainConstraintState *) lfirst(l);

        switch (con->constrainttype) {
            case DOM_CONSTRAINT_NOTNULL:
                if (isnull) {
                    errsave(escontext,
                            (errcode(ERRCODE_NOT_NULL_VIOLATION),
                             errmsg("domain %s does not allow null values",
                                    format_type_be(my_extra->domain_type)),
                             errdatatype(my_extra->domain_type)));
                    goto fail;
                }
                break;

            case DOM_CONSTRAINT_CHECK:
                // Create expression context if needed
                if (econtext == NULL) {
                    MemoryContext oldcontext = MemoryContextSwitchTo(my_extra->mcxt);
                    econtext = CreateStandaloneExprContext();
                    MemoryContextSwitchTo(oldcontext);
                    my_extra->econtext = econtext;
                }

                // Set up value for CoerceToDomainValue nodes
                econtext->domainValue_datum =
                    MakeExpandedObjectReadOnly(value, isnull,
                                              my_extra->constraint_ref.tcache->typlen);
                econtext->domainValue_isNull = isnull;

                // Execute CHECK constraint
                if (!ExecCheck(con->check_exprstate, econtext)) {
                    errsave(escontext,
                            (errcode(ERRCODE_CHECK_VIOLATION),
                             errmsg("value for domain %s violates check constraint \"%s\"",
                                    format_type_be(my_extra->domain_type),
                                    con->name),
                             errdomainconstraint(my_extra->domain_type,
                                               con->name)));
                    goto fail;
                }
                break;

            default:
                elog(ERROR, "unrecognized constraint type: %d",
                     (int) con->constrainttype);
                break;
        }
    }

fail:
    // Cleanup expression context resources
    if (econtext) {
        ReScanExprContext(econtext);
    }
}
```