# RelationGetExclusionInfo

## Location
[src/backend/utils/cache/relcache.c:5596-5727](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relcache.c#L5596-L5727)

## Overview
Retrieves detailed information about an exclusion constraint associated with an index, including the exclusion operators, their underlying function OIDs, and strategy numbers.

## Definition

```c
void
RelationGetExclusionInfo(Relation indexRelation,
						 Oid **operators,
						 Oid **procs,
						 uint16 **strategies)
```
## Detailed Description
This function extracts and caches information about an exclusion constraint that is associated with the given index relation. It should only be called for indexes that are known to have an associated exclusion constraint.

The function returns three arrays (allocated in the caller's memory context):
1. **operators**: Array of exclusion operator OIDs used in the constraint
2. **procs**: Array of underlying function OIDs for those operators  
3. **strategies**: Array of strategy numbers for the operators in the index's operator classes

The function first checks if the information is already cached in the index relation's cache entry (, , ). If cached, it simply copies the data and returns.

If not cached, it searches the  system catalog to find the exclusion constraint record associated with the index. It uses the constraint's  (parent relation OID) and scans for  type constraints with matching .

Once found, it extracts the operator OIDs from the  array field, then looks up the corresponding function OIDs and strategy numbers. Finally, it caches all this information in the index relation's cache context for future use.

## Parameters / Member Variables
- : The index relation that has an exclusion constraint
- : Output parameter - pointer to array of exclusion operator OIDs
- : Output parameter - pointer to array of underlying function OIDs  
- : Output parameter - pointer to array of strategy numbers

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes
  - [palloc](../p/palloc.md)/memcpy
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - table_open/table_close
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [fastgetattr](../f/fastgetattr.md)
  - DatumGetArrayTypeP
  - ARR_DIMS/ARR_NDIM/ARR_HASNULL/ARR_ELEMTYPE/ARR_DATA_PTR
  - [get_opcode](../g/get_opcode.md)
  - [get_op_opfamily_strategy](../g/get_op_opfamily_strategy.md)
- Called from (representative examples):
  - [BuildIndexInfo](../B/BuildIndexInfo.md)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)

## Notes and Other Information
- Should only be called for indexes with known exclusion constraints
- Results are cached in the index relation's cache context () for performance
- The function performs validation on the  array to ensure it's a well-formed 1-D OID array
- Strategy number lookup should not fail since operators are validated at index creation time
- Uses  when scanning the constraint catalog
- The returned arrays are allocated in the caller's memory context and should be freed when no longer needed