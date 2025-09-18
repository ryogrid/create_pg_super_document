# DomainConstraintType

## Location
src/include/nodes/execnodes.h: 1005 - 1006

## Overview
An enumeration that specifies the type of domain constraint to be checked during domain value coercion operations in PostgreSQL.

## Definition


## Detailed Description
DomainConstraintType is used to distinguish between different types of domain constraints that need to be evaluated when coercing values to a domain type. PostgreSQL domains can have two types of constraints: NOT NULL constraints that prevent null values, and CHECK constraints that enforce custom validation rules through boolean expressions. This enumeration allows the constraint checking machinery to handle each type appropriately during runtime evaluation.

## Parameters / Member Variables
- : Indicates a NOT NULL constraint that prevents null values from being assigned to the domain
- : Indicates a CHECK constraint with a custom boolean expression that must evaluate to true

## Dependencies
- Functions called/Symbols referenced: (None - this is a simple enumeration)
- Called from (representative examples):
  - [DomainConstraintState](DomainConstraintState.md) (used as constrainttype field)
  - execExpr.c:ExecEvalCoerceToDomain() (switch cases at lines 3406, 3410)
  - typcache.c:GetDomainConstraints() (assignments at lines 1121, 1200)
  - domains.c:domain_check() (switch cases at lines 153, 164)

## Notes and Other Information
This enumeration is part of the execution node infrastructure and follows the PostgreSQL naming convention for execution state types. It works in conjunction with DomainConstraintState to represent individual domain constraints during expression evaluation. The enum is specifically designed to support PostgreSQL's domain constraint checking mechanism, which validates values against domain-specific rules at runtime.