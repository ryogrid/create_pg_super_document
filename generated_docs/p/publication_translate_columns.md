# publication_translate_columns

## Location
src/backend/catalog/pg_publication.c: 502 - 569

## Overview
Translates a list of column names to an array of attribute numbers and validates that each attribute is appropriate for inclusion in a publication column list.

## Definition
```c
static void publication_translate_columns(Relation targetrel, List *columns, int *natts, AttrNumber **attrs)
```

## Detailed Description
This function performs the critical task of converting human-readable column names into internal attribute numbers while enforcing publication column list restrictions. It takes a list of column name strings and produces a sorted array of AttrNumber values that can be stored in the catalog.

The function performs several validation checks:
1. Verifies that each column name actually exists in the relation
2. Prohibits system columns (negative attribute numbers)
3. Prohibits generated columns, which cannot be meaningfully replicated
4. Prevents duplicate columns in the list
5. Sorts the resulting attribute numbers for consistent catalog representation

The function uses a Bitmapset during processing to efficiently check for duplicates, then produces a sorted AttrNumber array as output. If no column list is provided (columns is NULL), the function returns early without setting the output parameters.

## Parameters / Member Variables
- `targetrel`: The relation for which column names are being translated
- `columns`: List of column name strings to translate (can be NULL)
- `natts`: Output parameter - number of attributes in the resulting array  
- `attrs`: Output parameter - pointer to allocated array of attribute numbers

## Dependencies
- Functions called/Symbols referenced:
  - [get_attnum](../g/get_attnum.md)
  - AttrNumberIsForUserDefinedAttr
  - [bms_is_member](../b/bms_is_member.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [compare_int16](../c/compare_int16.md)
  - qsort
  - [bms_free](../b/bms_free.md)
- Called from (representative examples):
  - [publication_add_relation](publication_add_relation.md) (src/backend/catalog/pg_publication.c:403)
  - published_rel (src/backend/catalog/pg_publication.c:51)

## Notes and Other Information
- This is a static function with internal linkage, only accessible within pg_publication.c
- The function allocates memory for the attribute array using palloc() - caller is responsible for cleanup
- Attribute numbers are not offset by FirstLowInvalidHeapAttributeNumber since system columns are forbidden
- The resulting array is always sorted using qsort() with compare_int16 for consistent catalog representation
- Additional validation regarding replica identity is performed later by other functions like pub_collist_contains_invalid_column
- Uses a temporary Bitmapset for efficient duplicate detection, which is freed before returning
- Error messages provide specific details about which column and relation caused the problem
- Location: src/backend/catalog/pg_publication.c:502-569