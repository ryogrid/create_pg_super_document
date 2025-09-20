# EnumItem

## Location
[src/backend/utils/cache/typcache.c:136-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L136-L137)

## Overview
EnumItem is a structure that represents a single enumeration value within PostgreSQL's type cache system, storing the OID and sort order for efficient enum comparison operations.

## Definition

```c
typedef struct TypeCacheEnumData
{
	Oid			bitmap_base;	/* OID corresponding to bit 0 of bitmapset */
	Bitmapset  *sorted_values;	/* Set of OIDs known to be in order */
	int			num_values;		/* total number of values in enum */
	EnumItem	enum_values[FLEXIBLE_ARRAY_MEMBER];
} TypeCacheEnumData;
```
## Detailed Description
EnumItem is a private data structure used within the PostgreSQL type cache system specifically for supporting efficient comparisons of enum values. It is part of the enum caching mechanism that allows PostgreSQL to perform fast ordering operations on user-defined enum types without repeatedly querying the system catalogs.

Each EnumItem represents one possible value of an enum type, storing both its unique identifier (OID) and its logical position within the enum's ordering (sort_order). The sort_order field corresponds to the enumsortorder column from the pg_enum system catalog, which defines the logical ordering of enum values that may differ from their OID creation order.

EnumItem structures are typically stored in arrays within TypeCacheEnumData and are maintained in OID-sorted order to enable efficient binary search operations for enum value lookups and comparisons.

## Parameters / Member Variables
- `enum_oid`: The Object Identifier (OID) that uniquely identifies this specific enum value in the database system
- `sort_order`: A floating-point value that defines the logical ordering position of this enum value, allowing for custom enum ordering that differs from OID creation order

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a data structure)
  
- Used by (representative examples):
  - [TypeCacheEnumData](../T/TypeCacheEnumData.md) (as array member enum_values)
  - [compare_values_of_enum](../c/compare_values_of_enum.md) (for enum comparison operations)
  - [load_enum_cache_data](../l/load_enum_cache_data.md) (for populating enum cache data)
  - [find_enumitem](../f/find_enumitem.md) (for locating specific enum values)
  - [enum_oid_cmp](../e/enum_oid_cmp.md) (for sorting and searching operations)

## Notes and Other Information
- [EnumItem](EnumItem.md) structures are allocated and managed within CacheMemoryContext to ensure they persist for the lifetime of the backend process
- The arrays of EnumItem are kept sorted by enum_oid to enable efficient binary search via bsearch()
- The sort_order field allows PostgreSQL to support custom enum ordering that can be different from the OID creation order, which is important for ALTER TYPE ... ADD VALUE operations
- This structure is part of PostgreSQL's optimization strategy to avoid repeated system catalog lookups when performing enum comparisons
- The structure is used internally by the type cache system and is not exposed to user-level SQL operations