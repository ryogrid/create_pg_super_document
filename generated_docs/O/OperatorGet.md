# OperatorGet

## Location
[src/backend/catalog/pg_operator.c:115-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_operator.c#L115-L153)

## Overview
Looks up an operator in the system catalogs using exact specification (name, namespace, and operand types) and determines whether it's fully defined or just a shell.

## Definition

```c
static Oid
OperatorGet(const char *operatorName,
            Oid operatorNamespace,
            Oid leftObjectId,
            Oid rightObjectId,
            bool *defined)
```

## Detailed Description
OperatorGet performs a precise lookup of an operator in the pg_operator system catalog using all four identifying components: operator name, namespace, left operand type, and right operand type. The function not only returns the operator's OID if found, but also indicates whether the operator is fully defined or is merely a "shell" operator.

Shell operators are placeholder entries created during operator definition to handle forward references (such as commutator pairs where each operator references the other). A shell operator has an invalid oprcode field, indicating that while the operator entry exists, its implementation function hasn't been specified yet.

## Parameters / Member Variables
- `operatorName`: C string containing the operator name to search for
- `operatorNamespace`: OID of the namespace containing the operator
- `leftObjectId`: OID of the left operand type (InvalidOid for prefix operators)
- `rightObjectId`: OID of the right operand type (InvalidOid for postfix operators)
- `defined`: Pointer to boolean that will be set to indicate if operator is fully defined

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache4: System cache lookup with 4 key values
  - PointerGetDatum: Converts pointer to Datum for cache lookup
  - ObjectIdGetDatum: Converts OID to Datum for cache lookup
  - GETSTRUCT: Macro to extract structure from HeapTuple
  - RegProcedureIsValid: Checks if procedure OID is valid
  - ReleaseSysCache: Releases system cache tuple

## Notes and Other Information
- Uses OPERNAMENSP cache for efficient operator lookup
- Shell operators have invalid oprcode (implementation function) but valid OID
- Returns InvalidOid and sets defined=false if operator not found
- Essential for operator resolution during SQL parsing and execution
- Used internally by higher-level operator lookup functions

## Simplified Source

```c
static Oid
OperatorGet(const char *operatorName, Oid operatorNamespace,
            Oid leftObjectId, Oid rightObjectId, bool *defined)
{
    // Search operator catalog by name, types, and namespace
    HeapTuple tup = SearchSysCache4(OPERNAMENSP,
                                   PointerGetDatum(operatorName),
                                   ObjectIdGetDatum(leftObjectId),
                                   ObjectIdGetDatum(rightObjectId),
                                   ObjectIdGetDatum(operatorNamespace));

    if (HeapTupleIsValid(tup)) {
        // Operator found - extract info and check if fully defined
        Form_pg_operator oprform = (Form_pg_operator) GETSTRUCT(tup);

        Oid operatorObjectId = oprform->oid;
        *defined = RegProcedureIsValid(oprform->oprcode); // Has implementation function?

        ReleaseSysCache(tup);
        return operatorObjectId;
    } else {
        // Operator not found
        *defined = false;
        return InvalidOid;
    }
}
```