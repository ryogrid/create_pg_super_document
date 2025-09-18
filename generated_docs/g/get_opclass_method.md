# get_opclass_method

## Location
src/backend/utils/cache/lsyscache.c: 1260 - 1284

## Overview
Retrieves the OID of the index access method that an operator class belongs to, providing the linkage between operator classes and their underlying index access methods.

## Definition
```c
Oid get_opclass_method(Oid opclass)
```

## Detailed Description
This function performs a system catalog lookup to determine which index access method (such as btree, hash, gist, gin, etc.) is associated with a given operator class. It accesses the pg_opclass system catalog through the system cache and retrieves the opcmethod field, which contains the OID of the access method. Unlike some similar functions, this function throws an ERROR if the operator class is not found, making it suitable for cases where the operator class is expected to exist.

## Parameters / Member Variables
- `opclass`: The OID of the operator class to look up

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1 (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - elog (error logging and reporting)
  - GETSTRUCT (tuple structure access)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_opclass (catalog tuple structure)
- Called from (representative examples):
  - get_equal_strategy_number (replication equality strategy lookup)

## Notes and Other Information
- Throws an ERROR (via elog) if the specified operator class OID is not found, unlike some similar functions that return false
- Uses system cache for performance optimization when accessing pg_opclass catalog
- Returns the raw OID of the access method, which can be used to look up access method properties
- Essential for determining which index access method operations are available for a given operator class
- The access method OID can be used with other system functions to get access method names and properties