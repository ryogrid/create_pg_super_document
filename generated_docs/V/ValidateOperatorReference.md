# ValidateOperatorReference

## Location
[src/backend/commands/operatorcmds.c:372-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/operatorcmds.c#L372-L412)

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
  - [OperatorLookup](../O/OperatorLookup.md) (operator lookup by name and signature)
  - [op_signature_string](../o/op_signature_string.md) (format operator signature for error messages)
  - [object_ownercheck](../o/object_ownercheck.md) (ownership verification)
  - [aclcheck_error](../a/aclcheck_error.md) (error reporting for permission failures)
  - [NameListToString](../N/NameListToString.md) (name formatting for error messages)
- Called from (representative examples):
  - [AlterOperator](../A/AlterOperator.md) (twice - for commutator and negator validation)

## Notes and Other Information
- The function is static and only used within operatorcmds.c
- Requires that the operator be owned by the current user
- Rejects shell operators (incomplete operator definitions) as a policy choice
- Error messages are designed to match those in parse_oper.c for consistency
- Returns the OID of the validated operator
- Used specifically in ALTER OPERATOR commands for setting commutator and negator relationships
- The ownership requirement ensures only the operator owner can modify its properties

## Simplified Source

```c
static Oid
ValidateOperatorReference(List *name, Oid leftTypeId, Oid rightTypeId)
{
    Oid oid;
    bool defined;

    // Look up operator by name and type signature
    oid = OperatorLookup(name, leftTypeId, rightTypeId, &defined);

    // Check if operator exists
    if (!OidIsValid(oid))
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("operator does not exist: %s",
                        op_signature_string(name, leftTypeId, rightTypeId))));

    // Reject shell operators (incomplete definitions)
    if (!defined)
        ereport(ERROR,
                (errcode(ERRCODE_UNDEFINED_FUNCTION),
                 errmsg("operator is only a shell: %s",
                        op_signature_string(name, leftTypeId, rightTypeId))));

    // Verify current user owns the operator
    if (!object_ownercheck(OperatorRelationId, oid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_OPERATOR, NameListToString(name));

    return oid;
}
```