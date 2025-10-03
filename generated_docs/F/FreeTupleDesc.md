# FreeTupleDesc

## Location
[src/backend/access/common/tupdesc.c:331-387](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tupdesc.c#L331-L387)

## Overview
Completely deallocates a TupleDesc and all its associated constraint structures, including default values, missing values, and check constraints.

## Definition

```c
void
FreeTupleDesc(TupleDesc tupdesc)
```
## Detailed Description
This function performs a complete cleanup of a tuple descriptor by deallocating all memory associated with it and its constraint structures. It systematically frees default value expressions, missing value data, check constraint information, and finally the tuple descriptor itself. The function includes proper handling of pass-by-reference data types in missing values and validates that the reference count is non-positive before proceeding with deallocation.

## Parameters / Member Variables
- `tupdesc`: The TupleDesc to free, along with all its associated constraint data
## Dependencies
- Functions called/Symbols referenced:
  - [AttrDefault](../A/AttrDefault.md)
  - [AttrMissing](../A/AttrMissing.md)
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

## Simplified Source

```c
// Simplified version of FreeTupleDesc
void FreeTupleDesc(TupleDesc tupdesc) {
    // Validate reference count
    Assert(tupdesc->tdrefcount <= 0);

    // Free constraint structures if they exist
    if (tupdesc->constr) {
        // Free default value expressions
        if (tupdesc->constr->num_defval > 0) {
            AttrDefault *attrdef = tupdesc->constr->defval;
            for (int i = tupdesc->constr->num_defval - 1; i >= 0; i--)
                pfree(attrdef[i].adbin);
            pfree(attrdef);
        }

        // Free missing value data
        if (tupdesc->constr->missing) {
            AttrMissing *attrmiss = tupdesc->constr->missing;
            for (int i = tupdesc->natts - 1; i >= 0; i--) {
                if (attrmiss[i].am_present &&
                    !TupleDescAttr(tupdesc, i)->attbyval)
                    pfree(DatumGetPointer(attrmiss[i].am_value));
            }
            pfree(attrmiss);
        }

        // Free check constraints
        if (tupdesc->constr->num_check > 0) {
            ConstrCheck *check = tupdesc->constr->check;
            for (int i = tupdesc->constr->num_check - 1; i >= 0; i--) {
                pfree(check[i].ccname);
                pfree(check[i].ccbin);
            }
            pfree(check);
        }

        pfree(tupdesc->constr);
    }

    // Finally free the tuple descriptor itself
    pfree(tupdesc);
}
```

Key simplifications made:
- Removed detailed comments for clarity
- Consolidated the nested freeing operations
- Maintained proper reference count validation
- Preserved all essential cleanup operations for constraints, defaults, and missing values