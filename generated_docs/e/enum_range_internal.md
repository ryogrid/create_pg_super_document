# enum_range_internal

## Location
[src/backend/utils/adt/enum.c:547-616](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/enum.c#L547-L616)

## Overview
Generates an array containing enum values within a specified range by scanning pg_enum in sorted order and filtering based on lower and upper bounds.

## Definition

```c
static ArrayType *
enum_range_internal(Oid enumtypoid, Oid lower, Oid upper)
```
## Detailed Description
This internal function is the core implementation for PostgreSQL's enum_range() SQL function variants. It scans the pg_enum system catalog to retrieve enum values of a specific enum type within an optional range defined by lower and upper bounds.

The function uses the pg_enum_typid_sortorder_index to ensure enum values are retrieved in their defined sort order. It deliberately avoids using the system cache (syscache) to prevent issues during enum renumbering operations, as documented in RenumberEnumType.

Key implementation details:
- Performs an ordered scan using systable_beginscan_ordered() to maintain enum sort order
- Dynamically allocates and grows the result array as needed (starting with 64 elements)
- Validates each enum value using check_safe_enum_use() to prevent corruption during transactions
- Supports open ranges when lower/upper bounds are InvalidOid
- Returns a properly constructed ArrayType suitable for PostgreSQL's array system

## Parameters / Member Variables
- : The OID of the enum type whose values should be retrieved
- : The OID of the lower bound enum value (InvalidOid for no lower bound)  
- : The OID of the upper bound enum value (InvalidOid for no upper bound)

## Dependencies
- Functions called/Symbols referenced:
  - [ScanKeyInit](../S/ScanKeyInit.md)
  - [table_open](../t/table_open.md)/table_close
  - [index_open](../i/index_open.md)/index_close
  - [systable_beginscan_ordered](../s/systable_beginscan_ordered.md)/systable_endscan_ordered
  - [systable_getnext_ordered](../s/systable_getnext_ordered.md)
  - [check_safe_enum_use](../c/check_safe_enum_use.md)
  - [palloc](../p/palloc.md)/repalloc/pfree
  - [construct_array](../c/construct_array.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
- Called from:
  - [enum_range_bounds](enum_range_bounds.md) (2-argument enum_range variant)
  - [enum_range_all](enum_range_all.md) (1-argument enum_range variant)

## Notes and Other Information
- Uses pg_enum_typid_sortorder_index for ordered scanning, not syscache
- Implements dynamic array growth to handle enums of arbitrary size
- Safety check via check_safe_enum_use prevents issues with uncommitted enum values
- Hardwires knowledge of Oid representation details in construct_array call
- Supports both bounded and unbounded range queries through InvalidOid parameters
- Part of PostgreSQL's enum type system infrastructure in src/backend/utils/adt/enum.c