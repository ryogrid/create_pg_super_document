# op_input_types

## Location
[src/backend/utils/cache/lsyscache.c:1358-1385](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1358-L1385)

## Overview
Retrieves the left and right input data types for a given operator from the system catalog.

## Definition

```c
void
op_input_types(Oid opno, Oid *lefttype, Oid *righttype)
```
## Detailed Description
This function looks up an operator in the pg_operator system catalog using its OID and returns the data types of its left and right operands. It's a utility function in the system cache layer that provides a convenient interface for accessing operator type information. The function uses the system cache (syscache) for efficient lookup and will throw an error if the operator OID is not found.

## Parameters / Member Variables
- `opno`: The OID of the operator to look up
- `*lefttype`: Output parameter that receives the OID of the left operand data type (InvalidOid if not applicable)
- `*righttype`: Output parameter that receives the OID of the right operand data type (InvalidOid if not applicable)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_operator
- Called from (representative examples):
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)
  - [typeDepNeeded](../t/typeDepNeeded.md)
  - [process_equivalence](../p/process_equivalence.md)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md)
  - [initialize_mergeclause_eclasses](../i/initialize_mergeclause_eclasses.md)
  - [ri_HashCompareOp](../r/ri_HashCompareOp.md)

## Notes and Other Information
- The function will raise an ERROR if the operator OID is not found in the system catalog
- For unary operators, one of the output type parameters will be set to InvalidOid
- This is part of the lsyscache module which provides cached access to system catalog information
- The function is commonly used in query planning and optimization phases

## Simplified Source

```c
void
op_input_types(Oid opno, Oid *lefttype, Oid *righttype)
{
    HeapTuple   tp;
    Form_pg_operator optup;

    // Look up operator in system catalog
    tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for operator %u", opno);

    // Extract left and right operand types
    optup = (Form_pg_operator) GETSTRUCT(tp);
    *lefttype = optup->oprleft;
    *righttype = optup->oprright;

    ReleaseSysCache(tp);
}
```