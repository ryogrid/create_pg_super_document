# ValidateOperatorReference

## Location
src/backend/commands/operatorcmds.c: 372 - 412

## Overview
ValidateOperatorReference looks up an operator by name and type signature, verifying it exists as a complete operator and that the current user owns it for use in operator alterations.

## Definition
```c
static Oid ValidateOperatorReference(List *name, Oid leftTypeId, Oid rightTypeId)
```

## Detailed Description
This static function validates operator references used in ALTER OPERATOR commands when setting commutator or negator relationships. It looks up an operator by its qualified name and left/right operand type OIDs, ensuring the operator is fully defined (not a shell operator) and owned by the current user. This validation is necessary because only the owner of an operator should be able to modify its relationships with other operators.

Shell operators are incomplete operator definitions that exist in the system catalog but lack an implementation function. Rejecting shell operators helps catch configuration mistakes and ensures that only fully functional operators can be used in commutator/negator relationships.

## Parameters / Member Variables
- `name`: List containing the qualified name of the operator to validate
- `leftTypeId`: OID of the left operand type (InvalidOid for prefix operators)
- `rightTypeId`: OID of the right operand type

## Dependencies
- Functions called/Symbols referenced:
  - OperatorLookup (operator lookup by name and signature)
  - op_signature_string (format operator signature for error messages)
  - object_ownercheck (ownership verification)
  - aclcheck_error (error reporting for permission failures)
  - NameListToString (name formatting for error messages)
- Called from (representative examples):
  - AlterOperator (twice - for commutator and negator validation)

## Notes and Other Information
- The function is static and only used within operatorcmds.c
- Requires that the operator be owned by the current user
- Rejects shell operators (incomplete operator definitions) as a policy choice
- Error messages are designed to match those in parse_oper.c for consistency
- Returns the OID of the validated operator
- Used specifically in ALTER OPERATOR commands for setting commutator and negator relationships
- The ownership requirement ensures only the operator owner can modify its properties