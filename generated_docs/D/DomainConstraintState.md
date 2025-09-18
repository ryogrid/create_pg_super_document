# DomainConstraintState

## Location
src/include/nodes/execnodes.h: 1007 - 1014

## Overview
DomainConstraintState represents the execution state for validating domain constraints during type coercion operations, supporting both NOT NULL and CHECK constraints.

## Definition


## Detailed Description
DomainConstraintState manages the runtime validation of domain constraints when values are coerced to domain types. PostgreSQL domains allow users to create custom data types with additional constraints beyond the base type. This structure holds the execution state needed to evaluate these constraints efficiently during query execution. It supports two types of constraints: NOT NULL constraints that prevent null values, and CHECK constraints that evaluate boolean expressions against the domain value.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : Type of domain constraint (DOM_CONSTRAINT_NOTNULL or DOM_CONSTRAINT_CHECK)
- : Human-readable name of the constraint, used in error messages when constraint violations occur
- : For CHECK constraints, the boolean expression that must evaluate to true for valid values
- : Compiled execution state for the check_expr, or NULL for NOT NULL constraints

## Dependencies
- Functions called/Symbols referenced:
  - DomainConstraintType
  - NodeTag
  - Expr
  - ExprState
- Called from (representative examples):
  - ExecInitCoerceToDomain
  - domain_check_input
  - load_domaintype_info
  - prep_domain_constraints

## Notes and Other Information
DomainConstraintState is considered part of an ExprState tree despite not having a directly associated plan-tree node, following PostgreSQL's naming convention for execution state structures. The structure is optimized for repeated constraint checking during query execution, with the check_exprstate providing compiled expression evaluation for CHECK constraints. Domain constraints are evaluated during type coercion operations, ensuring data integrity at the type system level.