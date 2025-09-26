# CompareIndexInfo

## Location
[src/backend/catalog/index.c:2511-2641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L2511-L2641)

## Overview
CompareIndexInfo determines whether two index definitions are equivalent by comparing their structural properties, expressions, predicates, and metadata.

## Definition

```c
bool
CompareIndexInfo(const IndexInfo *info1, const IndexInfo *info2,
				 const Oid *collations1, const Oid *collations2,
				 const Oid *opfamilies1, const Oid *opfamilies2,
				 const AttrMap *attmap)
```
## Detailed Description
CompareIndexInfo performs a comprehensive comparison of two IndexInfo structures to determine if they represent functionally equivalent indexes that could exist on different tables. The function systematically checks all relevant index properties including uniqueness constraints, access methods, attribute mappings, expressions, and partial index predicates. This is particularly useful during operations like table partitioning where indexes need to be matched across related tables. The function uses an attribute map to handle cases where column numbers differ between tables but the logical structure remains the same.

## Parameters / Member Variables
- : First IndexInfo structure to compare
- : Second IndexInfo structure to compare  
- : Array of collation OIDs for the first index
- : Array of collation OIDs for the second index
- : Array of operator family OIDs for the first index
- : Array of operator family OIDs for the second index
- : Attribute mapping structure to handle column number differences between tables

## Dependencies
- Functions called/Symbols referenced:
  - [IndexInfo](../I/IndexInfo.md) (structure type)
  - [AttrMap](../A/AttrMap.md) (structure type)
  - InvalidAttrNumber (constant)
  - [map_variable_attnos](../m/map_variable_attnos.md) (function)
  - [equal](../e/equal.md) (function)
- Called from (representative examples):
  - [DefineIndex](../D/DefineIndex.md)
  - [AttachPartitionEnsureIndexes](../A/AttachPartitionEnsureIndexes.md)
  - [ATExecAttachPartitionIdx](../A/ATExecAttachPartitionIdx.md)

## Notes and Other Information
- The function requires collations and opfamilies to be passed separately, which the comments note as a kludge that could be improved
- Expression indexes and partial index predicates are compared by mapping variable attribute numbers and using structural equality
- Exclusion constraint indexes are not currently supported for comparison
- The attribute map should be built using build_attrmap_by_name(index2, index1) as noted in comments
- The function performs early returns on any mismatch to optimize performance