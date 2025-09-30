# sort_order_cmp

## Location
[src/backend/catalog/pg_enum.c:797-812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_enum.c#L797-L812)

## Overview
A static comparison function used by qsort to order pg_enum tuples by their enumsortorder field.

## Definition

```c
enum en1 = (Form_pg_enum) GETSTRUCT(v1);
```
## Detailed Description
This function serves as a comparison callback for the qsort() function when sorting enum value tuples from the pg_enum catalog table. It compares two HeapTuple pointers by extracting their Form_pg_enum structures and comparing their enumsortorder fields. The function returns a standard comparison result: negative if the first element should come before the second, positive if it should come after, and zero if they are equal.

The function is specifically designed to work with PostgreSQL's enum type system, where enum values have an associated sort order that determines their relative ordering within the enum type.

## Parameters / Member Variables
- : Pointer to the first HeapTuple pointer to compare
- : Pointer to the second HeapTuple pointer to compare

## Dependencies
- Functions called/Symbols referenced:
  - Form_pg_enum (type definition for pg_enum catalog structure)
  - GETSTRUCT (macro to extract structure from HeapTuple)
- Called from (representative examples):
  - [AddEnumLabel](../A/AddEnumLabel.md) (at src/backend/catalog/pg_enum.c:369)

## Notes and Other Information
- This is a static function, only accessible within the pg_enum.c compilation unit
- The function follows the standard qsort comparison function contract
- Used specifically in the context of adding new enum labels to maintain proper ordering
- The comparison is based on the enumsortorder field, which is a float4 value that determines the logical ordering of enum values

## Simplified Source

```c
static int
sort_order_cmp(const void *p1, const void *p2)
{
    // qsort comparison function for enum tuples by sort order
    HeapTuple v1 = *((const HeapTuple *) p1);
    HeapTuple v2 = *((const HeapTuple *) p2);
    Form_pg_enum en1 = (Form_pg_enum) GETSTRUCT(v1);
    Form_pg_enum en2 = (Form_pg_enum) GETSTRUCT(v2);

    // Compare enumsortorder fields - standard three-way comparison
    if (en1->enumsortorder < en2->enumsortorder)
        return -1;
    else if (en1->enumsortorder > en2->enumsortorder)
        return 1;
    else
        return 0;
}
```