# ExecInitCoerceToDomain

## Location
[src/backend/executor/execExpr.c:3346-3500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execExpr.c#L3346-L3500)

## Overview
Prepares evaluation of a CoerceToDomain expression by setting up domain constraint validation including NOT NULL and CHECK constraints.

## Definition


## Detailed Description
ExecInitCoerceToDomain initializes the execution framework for domain type coercion, which involves validating that a value meets all constraints defined for a domain type. It first evaluates the argument expression, then sets up constraint checking steps for each constraint associated with the domain.

The function handles two types of domain constraints: NOT NULL constraints (which simply check that the value is not null) and CHECK constraints (which evaluate arbitrary boolean expressions). For CHECK constraints, it manages memory allocation for workspace and handles the CoerceToDomainValue mechanism that allows constraint expressions to reference the value being checked. For varlena types, it ensures read-only access during constraint evaluation while preserving the original read-write expanded object for the final result.

## Parameters / Member Variables
- : ExprEvalStep structure to be configured for domain checking operations
- Usage

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