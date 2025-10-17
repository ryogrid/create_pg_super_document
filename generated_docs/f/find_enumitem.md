# find_enumitem

## Location
[src/backend/utils/cache/typcache.c:2705-2721](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L2705-L2721)

## Overview
Locates an EnumItem with a given OID within the enum type's cached data structure using binary search.

## Definition

```c
static EnumItem *
find_enumitem(TypeCacheEnumData *enumdata, Oid arg)
```
## Detailed Description
The  function performs a binary search to locate a specific enum value within the cached enum data structure. It searches through the  array in the  structure to find an  that matches the provided OID. The function uses the  comparison function to perform the binary search via the standard C library's  function. The implementation includes a safety check for empty arrays to prevent core dumps on certain Solaris versions.

## Parameters / Member Variables
- `*enumdata`: Pointer to the TypeCacheEnumData structure containing cached enum information
- `arg`: The OID of the enum value to search for
## Dependencies
- Functions called/Symbols referenced:
  - bsearch (C standard library function)
  - [enum_oid_cmp](../e/enum_oid_cmp.md) (comparison function for OID ordering)
- Data structures used:
  - [TypeCacheEnumData](../T/TypeCacheEnumData.md)
  - [EnumItem](../E/EnumItem.md)
- Called from (representative examples):
  - [compare_values_of_enum](../c/compare_values_of_enum.md) (multiple calls for enum value comparison)

## Notes and Other Information
- Returns NULL if the enum value is not found or if the enum data contains no values
- The function is static and only used within the typcache.c module
- Includes a specific workaround for Solaris systems where bsearch on zero items could cause crashes
- The enum_values array must be sorted by OID for binary search to work correctly
- Used primarily for enum value comparison operations in the type cache system

## Simplified Source

```c
static EnumItem *find_enumitem(TypeCacheEnumData *enumdata, Oid arg) {
    EnumItem search_key;

    // Safety check for empty array (Solaris compatibility)
    if (enumdata->num_values <= 0)
        return NULL;

    // Set up search key and perform binary search
    search_key.enum_oid = arg;
    return bsearch(&search_key, enumdata->enum_values, enumdata->num_values,
                   sizeof(EnumItem), enum_oid_cmp);
}
```