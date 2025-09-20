# RI_CompareHashEntry

## Location
[src/backend/utils/adt/ri_triggers.c:161-167](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ri_triggers.c#L161-L167)

## Overview
RI_CompareHashEntry is a hash table entry structure that caches equality comparison function information for referential integrity operations, storing both the comparison key and the precomputed function call information for efficient value comparisons.

## Definition

```c
typedef struct RI_CompareHashEntry
{
	RI_CompareKey key;
	bool		valid;			/* successfully initialized? */
	FmgrInfo	eq_opr_finfo;	/* call info for equality fn */
	FmgrInfo	cast_func_finfo;	/* in case we must coerce input */
} RI_CompareHashEntry;
```
## Detailed Description
RI_CompareHashEntry represents an entry in the comparison operator cache hash table used by PostgreSQL's referential integrity system. Each entry stores precomputed function call information for equality operations on specific data types, along with optional type coercion information. This structure eliminates the need to repeatedly look up function information during foreign key constraint checking, significantly improving performance for applications with frequent constraint validations.

## Parameters / Member Variables
- : RI_CompareKey structure that uniquely identifies the equality operator and data type combination
- : Boolean flag indicating whether the hash entry was successfully initialized and contains valid function information
- : FmgrInfo structure containing precomputed call information for the equality operator function
- : FmgrInfo structure containing call information for type coercion function, used when input values need to be converted before comparison

## Dependencies
- Functions called/Symbols referenced:
  - [RI_CompareKey](RI_CompareKey.md)
  - [FmgrInfo](../F/FmgrInfo.md) (PostgreSQL function manager info structure)
- Called from (representative examples):
  - [ri_InitHashTables](../r/ri_InitHashTables.md)
  - [ri_AttributesEqual](../r/ri_AttributesEqual.md)
  - [ri_HashCompareOp](../r/ri_HashCompareOp.md)

## Notes and Other Information
This structure is essential for optimizing referential integrity checking in PostgreSQL. The FmgrInfo structures cache function lookup results, allowing the system to call equality operators directly without repeated catalog lookups. The cast_func_finfo member handles cases where foreign key and primary key columns have compatible but not identical types, requiring type coercion before comparison. The valid flag ensures that only properly initialized entries are used, preventing errors during constraint validation operations.