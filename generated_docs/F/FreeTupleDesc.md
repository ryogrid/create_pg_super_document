# FreeTupleDesc

## Location
[src/backend/access/common/tupdesc.c:331-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L331-L387)

## Overview
Completely deallocates a TupleDesc and all its associated constraint structures, including default values, missing values, and check constraints.

## Definition


## Detailed Description
This function performs a complete cleanup of a tuple descriptor by deallocating all memory associated with it and its constraint structures. It systematically frees default value expressions, missing value data, check constraint information, and finally the tuple descriptor itself. The function includes proper handling of pass-by-reference data types in missing values and validates that the reference count is non-positive before proceeding with deallocation.

## Parameters / Member Variables
- : The TupleDesc to free, along with all its associated constraint data

## Dependencies
- Functions called/Symbols referenced:
  - [AttrDefault](../A/AttrDefault.md)
  - AttrMissing
  - [ConstrCheck](../C/ConstrCheck.md)
- Called from (representative examples):
  - [DecrTupleDescRefCount](../D/DecrTupleDescRefCount.md)
  - [ResOwnerReleaseTupleDesc](../R/ResOwnerReleaseTupleDesc.md)
  - [spgendscan](../s/spgendscan.md)
  - [AddNewAttributeTuples](../A/AddNewAttributeTuples.md)
  - [ExecMakeTableFunctionResult](../E/ExecMakeTableFunctionResult.md)
  - [RelationDestroyRelation](../R/RelationDestroyRelation.md)
  - [AtEOXact_RelationCache](../A/AtEOXact_RelationCache.md)
  - [TypeCacheRelCallback](../T/TypeCacheRelCallback.md)

## Notes and Other Information
- Validates tdrefcount <= 0 before freeing (should not free active references)
- Handles complex deallocation of constraint structures including:
  - Default value expressions (adbin strings)
  - Missing value data (handling pass-by-reference types)
  - Check constraint names and expressions
- Properly handles NULL constraint pointers
- Used extensively in cleanup scenarios across the PostgreSQL codebase
- Essential for preventing memory leaks in tuple descriptor management
- Must only be called when no active references exist to the tuple descriptor