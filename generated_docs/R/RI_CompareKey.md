# RI_CompareKey

## Location
[src/backend/utils/adt/ri_triggers.c:152-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L152-L156)

## Overview
RI_CompareKey is a structure that serves as a key for identifying cached comparison operators and data types used in referential integrity operations, enabling efficient lookup of how to compare two values of specific types.

## Definition

```c
typedef struct RI_CompareKey
{
	Oid			eq_opr;			/* the equality operator to apply */
	Oid			typeid;			/* the data type to apply it to */
} RI_CompareKey;
```
## Detailed Description
RI_CompareKey provides a composite key mechanism for caching equality comparison information in referential integrity operations. The structure combines an equality operator OID with a data type OID to uniquely identify how two values of a specific type should be compared for equality. This caching mechanism optimizes foreign key constraint checking by avoiding repeated lookups of appropriate comparison operators for the same data types.

## Parameters / Member Variables
- : OID of the equality operator function that should be used for comparing values of the specified type
- : OID of the data type for which this equality operator is applicable

## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [RI_CompareHashEntry](RI_CompareHashEntry.md) (as a member)
  - [ri_InitHashTables](../r/ri_InitHashTables.md)
  - [ri_HashCompareOp](../r/ri_HashCompareOp.md)

## Notes and Other Information
This structure is part of PostgreSQL's optimization strategy for referential integrity checking. By caching the mapping between data types and their appropriate equality operators, the system can quickly determine how to compare foreign key values with primary key values without repeatedly querying the system catalogs for operator information. The key is used in a hash table that stores RI_CompareHashEntry structures, providing fast access to comparison operator information during constraint validation operations.