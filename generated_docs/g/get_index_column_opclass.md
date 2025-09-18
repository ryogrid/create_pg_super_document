# get_index_column_opclass

## Location
[src/backend/utils/cache/lsyscache.c:3512-3554](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3512-L3554)

## Overview
Retrieves the operator class (opclass) for a specific column of an index, which defines the set of operators and support functions used for that indexed column.

## Definition
```c
Oid get_index_column_opclass(Oid index_oid, int attno)
```

## Detailed Description
This function looks up the operator class for a specified column within an index by querying the pg_index system catalog. An operator class defines the operators, comparison functions, and support functions that can be used with a particular data type in a specific index access method (like B-tree, GiST, etc.).

The function validates that the requested column number is within the valid range of key attributes for the index. Non-key attributes (included columns in covering indexes) do not have associated operator classes and will return InvalidOid. The function extracts the indclass array from the pg_index tuple and returns the operator class OID for the specified column position.

## Parameters / Member Variables
- `index_oid`: The OID of the index relation to query
- `attno`: The column number (1-based) for which to retrieve the operator class

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup with INDEXRELID)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - [SysCacheGetAttrNotNull](../S/SysCacheGetAttrNotNull.md) (attribute extraction from cached tuple)
  - [DatumGetPointer](../D/DatumGetPointer.md) (datum to pointer conversion)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_index (pg_index catalog structure)
  - oidvector (array type for storing OIDs)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
  - InvalidOid (constant for invalid OID)
- Called from (representative examples):
  - gistproperty
  - [spgproperty](../s/spgproperty.md)

## Notes and Other Information
- Returns InvalidOid if the index OID is invalid or not found
- Returns InvalidOid for non-key attributes (attno > indnkeyatts)
- Column numbers are 1-based, not 0-based
- The function includes assertions to validate input parameters
- Used primarily by index access method property functions
- Essential for determining index behavior and optimization strategies
- The indclass array contains operator class OIDs in column order