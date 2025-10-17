# get_op_rettype

## Location
[src/backend/utils/cache/lsyscache.c:1333-1357](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1333-L1357)

## Overview
Retrieves the result data type OID of a given operator, providing information about what type of value the operator returns when executed.

## Definition
```c
Oid get_op_rettype(Oid opno)
```

## Detailed Description
This function performs a system catalog lookup to determine the return type of a specified operator by accessing the pg_operator system catalog through the system cache. It retrieves the oprresult field, which contains the OID of the data type that the operator produces as its result. This information is essential for type checking and result type determination during query planning and execution. The function handles invalid operator OIDs gracefully by returning InvalidOid rather than throwing an error.

## Parameters / Member Variables
- `opno`: The OID of the operator whose return type is to be retrieved

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure access)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_operator (catalog tuple structure)
  - InvalidOid (null OID constant)
- Called from (representative examples):
  - [gistvalidate](gistvalidate.md) (GiST index validation)
  - [spgproperty](../s/spgproperty.md) (SP-GiST property checking)
  - [spgvalidate](../s/spgvalidate.md) (SP-GiST index validation)

## Notes and Other Information
- Returns InvalidOid if the specified operator OID is not found, allowing graceful error handling
- Uses system cache for performance optimization when accessing pg_operator catalog
- The returned type OID can be used with other type system functions to get detailed type information
- Essential for the type system to verify that operator results match expected types in expressions
- Used primarily by index access methods for validation and property checking
- Different from get_opcode which returns the implementation function, this returns the result type
- Important for ensuring type safety in operator expressions and index operations

## Simplified Source

```c
Oid get_op_rettype(Oid opno) {
    // Look up operator in system cache
    HeapTuple tp = SearchSysCache1(OPEROID, ObjectIdGetDatum(opno));

    if (HeapTupleIsValid(tp)) {
        // Extract operator structure and get result type
        Form_pg_operator optup = (Form_pg_operator) GETSTRUCT(tp);
        Oid result = optup->oprresult;
        ReleaseSysCache(tp);
        return result;
    } else {
        // Return invalid OID if operator not found
        return InvalidOid;
    }
}
```

This simplified version shows the function's straightforward catalog lookup pattern: search the operator cache, extract the result type from the operator tuple if found, clean up the cache reference, and return either the result type OID or InvalidOid if the operator doesn't exist.