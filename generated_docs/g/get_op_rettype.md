# get_op_rettype

## Location
src/backend/utils/cache/lsyscache.c: 1333 - 1357

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
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure access)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_operator (catalog tuple structure)
  - InvalidOid (null OID constant)
- Called from (representative examples):
  - gistvalidate (GiST index validation)
  - spgproperty (SP-GiST property checking)
  - spgvalidate (SP-GiST index validation)

## Notes and Other Information
- Returns InvalidOid if the specified operator OID is not found, allowing graceful error handling
- Uses system cache for performance optimization when accessing pg_operator catalog
- The returned type OID can be used with other type system functions to get detailed type information
- Essential for the type system to verify that operator results match expected types in expressions
- Used primarily by index access methods for validation and property checking
- Different from get_opcode which returns the implementation function, this returns the result type
- Important for ensuring type safety in operator expressions and index operations