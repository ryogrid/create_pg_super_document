# OperatorLookup

## Location
[src/backend/catalog/pg_operator.c:155-185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L155-L185)

## Overview
Looks up an operator given a possibly-qualified name and left and right type IDs, returning the operator OID and whether it's defined (not a shell).

## Definition
```c
Oid OperatorLookup(List *operatorName,
                   Oid leftObjectId,
                   Oid rightObjectId,
                   bool *defined)
```

## Detailed Description
This function searches for an operator in the PostgreSQL system catalog based on its name and operand types. It uses LookupOperName to find the operator and then checks whether it's a fully defined operator (has implementation code) or just a shell operator (placeholder without implementation). The function returns both the operator OID and sets a flag indicating whether the operator is defined.

## Parameters
- `operatorName`: List representing the qualified or unqualified name of the operator
- `leftObjectId`: OID of the left operand type
- `rightObjectId`: OID of the right operand type
- `defined`: Output parameter set to true if operator is defined (not a shell)

## Dependencies
- Functions called/Symbols referenced:
  - LookupOperName: Core operator lookup function
  - get_opcode: Get operator's implementation function
  - OidIsValid: Check if OID is valid
  - RegProcedureIsValid: Check if procedure is valid

## Notes and Other Information
- Returns InvalidOid if operator not found
- Shell operators exist as placeholders during CREATE OPERATOR processing
- Used during operator resolution in query planning and execution

## Simplified Source

```c
Oid OperatorLookup(List *operatorName,
                   Oid leftObjectId,
                   Oid rightObjectId,
                   bool *defined) {
    Oid operatorObjectId;
    RegProcedure oprcode;

    // Look up operator by name and operand types
    operatorObjectId = LookupOperName(NULL, operatorName,
                                     leftObjectId, rightObjectId,
                                     true, -1);
    if (!OidIsValid(operatorObjectId)) {
        *defined = false;
        return InvalidOid;
    }

    // Check if operator is defined (has implementation) or just a shell
    oprcode = get_opcode(operatorObjectId);
    *defined = RegProcedureIsValid(oprcode);

    return operatorObjectId;
}
```