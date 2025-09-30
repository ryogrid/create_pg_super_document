# ExecInitCoerceToDomain

## Location
[src/backend/executor/execExpr.c:3346-3500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3346-L3500)

## Overview
Prepares evaluation of a CoerceToDomain expression by setting up domain constraint validation including NOT NULL and CHECK constraints.

## Definition

```c
static void
ExecInitCoerceToDomain(ExprEvalStep *scratch, CoerceToDomain *ctest,
					   ExprState *state, Datum *resv, bool *resnull)
```
## Detailed Description
ExecInitCoerceToDomain initializes the execution framework for domain type coercion, which involves validating that a value meets all constraints defined for a domain type. It first evaluates the argument expression, then sets up constraint checking steps for each constraint associated with the domain.

The function handles two types of domain constraints: NOT NULL constraints (which simply check that the value is not null) and CHECK constraints (which evaluate arbitrary boolean expressions). For CHECK constraints, it manages memory allocation for workspace and handles the CoerceToDomainValue mechanism that allows constraint expressions to reference the value being checked. For varlena types, it ensures read-only access during constraint evaluation while preserving the original read-write expanded object for the final result.

## Parameters / Member Variables
- : ExprEvalStep structure to be configured for domain checking operations

  ctest [options]: CoerceToDomain node containing the coercion expression details
- : ExprState providing the expression evaluation context
- : Pointer to store the result Datum value
- : Pointer to store the result null flag

## Dependencies
- Functions called/Symbols referenced:
  - [ExecInitExprRec](ExecInitExprRec.md) (to initialize the argument expression and constraint expressions)
  - [InitDomainConstraintRef](../I/InitDomainConstraintRef.md) (to collect domain constraints)
  - [ExprEvalPushStep](ExprEvalPushStep.md) (to add execution steps)
  - [get_typlen](../g/get_typlen.md) (to determine if type is variable-length)
- Called from (representative examples):
  - [ExecInitExprRec](ExecInitExprRec.md) (during expression tree initialization)

## Notes and Other Information
- Constraints are baked into ExprState during initialization (not rechecked each evaluation)
- Handles nested domain constraints by saving/restoring innermost_domainval context
- For varlena types, creates MAKE_READONLY step to ensure constraint expressions see read-only values
- Uses DomainConstraintRef to manage constraint collection and memory context
- Supports error context reporting via escontext for constraint violations
- Allocates workspace lazily only when CHECK constraints are present

## Simplified Source

```c
static void ExecInitCoerceToDomain(ExprEvalStep *scratch, CoerceToDomain *ctest,
                                  ExprState *state, Datum *resv, bool *resnull)
{
    DomainConstraintRef *constraint_ref;
    Datum *domainval = NULL;
    bool *domainnull = NULL;
    ListCell *constraint_cell;

    // Initialize domain check step
    scratch->d.domaincheck.resulttype = ctest->resulttype;
    scratch->d.domaincheck.checkvalue = NULL;
    scratch->d.domaincheck.checknull = NULL;
    scratch->d.domaincheck.escontext = state->escontext;

    // Evaluate the argument expression into result variables
    ExecInitExprRec(ctest->arg, state, resv, resnull);

    // Collect domain constraints for this type
    constraint_ref = palloc(sizeof(DomainConstraintRef));
    InitDomainConstraintRef(ctest->resulttype, constraint_ref,
                           CurrentMemoryContext, false);

    // Process each domain constraint
    foreach(constraint_cell, constraint_ref->constraints) {
        DomainConstraintState *constraint = lfirst(constraint_cell);

        scratch->d.domaincheck.constraintname = constraint->name;

        switch (constraint->constrainttype) {
            case DOM_CONSTRAINT_NOTNULL:
                // Simple null check
                scratch->opcode = EEOP_DOMAIN_NOTNULL;
                ExprEvalPushStep(state, scratch);
                break;

            case DOM_CONSTRAINT_CHECK:
                // Allocate workspace for CHECK constraint evaluation
                if (scratch->d.domaincheck.checkvalue == NULL) {
                    scratch->d.domaincheck.checkvalue = palloc(sizeof(Datum));
                    scratch->d.domaincheck.checknull = palloc(sizeof(bool));
                }

                // Set up read-only access for varlena types if needed
                if (domainval == NULL) {
                    if (get_typlen(ctest->resulttype) == -1) {
                        // Variable-length type: create read-only copy
                        domainval = palloc(sizeof(Datum));
                        domainnull = palloc(sizeof(bool));

                        ExprEvalStep readonly_step = {0};
                        readonly_step.opcode = EEOP_MAKE_READONLY;
                        readonly_step.resvalue = domainval;
                        readonly_step.resnull = domainnull;
                        readonly_step.d.make_readonly.value = resv;
                        readonly_step.d.make_readonly.isnull = resnull;
                        ExprEvalPushStep(state, &readonly_step);
                    } else {
                        // Fixed-length type: use original values directly
                        domainval = resv;
                        domainnull = resnull;
                    }
                }

                // Set up constraint expression evaluation context
                Datum *save_domainval = state->innermost_domainval;
                bool *save_domainnull = state->innermost_domainnull;
                state->innermost_domainval = domainval;
                state->innermost_domainnull = domainnull;

                // Initialize constraint expression evaluation
                ExecInitExprRec(constraint->check_expr, state,
                               scratch->d.domaincheck.checkvalue,
                               scratch->d.domaincheck.checknull);

                // Restore previous context
                state->innermost_domainval = save_domainval;
                state->innermost_domainnull = save_domainnull;

                // Add constraint check step
                scratch->opcode = EEOP_DOMAIN_CHECK;
                ExprEvalPushStep(state, scratch);
                break;

            default:
                elog(ERROR, "unrecognized constraint type: %d", constraint->constrainttype);
        }
    }
}
```