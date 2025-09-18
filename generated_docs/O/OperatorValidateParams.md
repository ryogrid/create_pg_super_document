# OperatorValidateParams

## Location
src/backend/catalog/pg_operator.c: 556 - 621

## Overview
Validates that an operator definition with specific input/output types and attributes is logically consistent, enforcing PostgreSQL's rules about which operator attributes are allowed for different operator types.

## Definition
```c
void OperatorValidateParams(Oid leftTypeId,
                           Oid rightTypeId,
                           Oid operResultType,
                           bool hasCommutator,
                           bool hasNegator,
                           bool hasRestrictionSelectivity,
                           bool hasJoinSelectivity,
                           bool canMerge,
                           bool canHash)
```

## Detailed Description
This function enforces PostgreSQL's semantic rules for operator definitions by validating that requested operator attributes are compatible with the operator's type signature. It performs two main categories of validation:

1. **Binary Operator Constraints**: Certain attributes (commutator, join selectivity, merge join, hash join) are only meaningful for binary operators that take two arguments.

2. **Boolean Result Constraints**: Attributes like negator, selectivity functions, and join algorithms are only applicable to operators that return boolean values, as these are used in WHERE clauses and join conditions.

The function raises errors with appropriate error codes when invalid combinations are detected, preventing the creation of semantically inconsistent operator definitions.

## Parameters / Member Variables
- `leftTypeId`: OID of the left operand type (InvalidOid for unary prefix operators)
- `rightTypeId`: OID of the right operand type (InvalidOid for unary postfix operators)  
- `operResultType`: OID of the operator's return type
- `hasCommutator`: Whether the operator has a commutator operator defined
- `hasNegator`: Whether the operator has a negator operator defined
- `hasRestrictionSelectivity`: Whether the operator has a restriction selectivity function
- `hasJoinSelectivity`: Whether the operator has a join selectivity function
- `canMerge`: Whether the operator can be used in merge joins
- `canHash`: Whether the operator can be used in hash joins

## Dependencies
- Functions called/Symbols referenced:
  - OidIsValid (macro for checking valid OIDs)
  - ereport (error reporting function)
  - BOOLOID (boolean type OID constant)
- Called from (representative examples):
  - OperatorCreate (at src/backend/catalog/pg_operator.c:360)
  - AlterOperator (at src/backend/commands/operatorcmds.c:638)

## Notes and Other Information
- Used during both CREATE OPERATOR and ALTER OPERATOR commands to ensure semantic consistency
- The validation is designed to be independent for each attribute, allowing ALTER OPERATOR to only validate attributes being modified
- Unary operators (prefix or postfix) are identified by having either leftTypeId or rightTypeId as InvalidOid
- All advanced operator features (selectivity, join algorithms) require the operator to return boolean values since they're used in query optimization for WHERE clauses and joins