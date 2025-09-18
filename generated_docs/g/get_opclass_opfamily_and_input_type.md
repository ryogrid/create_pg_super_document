# get_opclass_opfamily_and_input_type

## Location
[src/backend/utils/cache/lsyscache.c:1235-1259](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L1235-L1259)

## Overview
Retrieves the operator family OID and input data type OID for a given operator class, providing essential metadata about the operator class structure.

## Definition


## Detailed Description
This function performs a system catalog lookup to extract key information about an operator class from the pg_opclass system catalog. It retrieves two critical pieces of metadata: the operator family that contains the operator class, and the data type that the operator class is designed to index. The function uses the system cache (CLAOID) for efficient access to operator class information and returns a boolean value indicating whether the lookup was successful.

## Parameters / Member Variables
- `opclass`: The OID of the operator class to look up
- `opfamily`: Output parameter - pointer to store the OID of the operator family containing this operator class
- `opcintype`: Output parameter - pointer to store the OID of the data type that this operator class indexes

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system cache lookup)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple structure access)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_opclass (catalog tuple structure)
- Called from (representative examples):
  - gistproperty (GiST index property checking)
  - [spgproperty](../s/spgproperty.md) (SP-GiST index property checking)  
  - [DefineIndex](../D/DefineIndex.md) (index definition processing)

## Notes and Other Information
- Returns false if the specified operator class OID is not found in the system catalog
- Uses system cache for performance optimization when accessing pg_opclass catalog
- The function safely handles invalid operator class OIDs by checking tuple validity
- Output parameters are only modified when the function returns true
- Essential for index access method implementations that need to understand operator class relationships