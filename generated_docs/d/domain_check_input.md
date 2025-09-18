# domain_check_input

## Location
src/backend/utils/adt/domains.c: 138 - 226

## Overview
Applies cached domain constraints to validate input values, executing NOT NULL and CHECK constraints separately.

## Definition


## Detailed Description
The  function validates a value against all domain constraints defined for a domain type. This function is roughly similar to the handling of CoerceToDomain nodes in execExpr*.c, but executes each constraint separately rather than compiling them in-line within a larger expression.

The function processes two types of domain constraints:
1. **NOT NULL constraints**: Checks if the value is null when the domain doesn't allow null values
2. **CHECK constraints**: Evaluates custom CHECK expressions against the input value

If the  parameter points to an ErrorSaveContext, any failures are reported there; otherwise they are reported via ereport. The function does not attempt soft reporting of errors raised during execution of CHECK constraints.

## Parameters / Member Variables
- : The Datum value to be validated against domain constraints
- : Boolean flag indicating whether the value is null
- : Pointer to DomainIOData structure containing cached constraint information
- : Node for error context handling (can be ErrorSaveContext or NULL)

## Dependencies
- Functions called/Symbols referenced:
  - DomainIOData (struct type)
  - UpdateDomainConstraintRef
  - DomainConstraintState
  - DOM_CONSTRAINT_NOTNULL
  - errsave
  - errdatatype
  - DOM_CONSTRAINT_CHECK
  - CreateStandaloneExprContext
  - MakeExpandedObjectReadOnly
  - ExecCheck
  - errdomainconstraint
  - ReScanExprContext

- Called from (representative examples):
  - domain_in (src/backend/utils/adt/domains.c:275)
  - domain_recv (src/backend/utils/adt/domains.c:331)
  - domain_check_internal (src/backend/utils/adt/domains.c:397)

## Notes and Other Information
- The function creates an ExprContext lazily when needed for CHECK constraint evaluation
- Values are protected against modification during constraint checking using MakeExpandedObjectReadOnly
- The function performs cleanup by calling ReScanExprContext to avoid leaking non-memory resources
- Constraint validation follows a fail-fast approach - the first failed constraint causes the function to jump to cleanup
- The function updates domain constraints before validation to ensure they are current