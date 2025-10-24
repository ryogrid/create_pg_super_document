# sqlda_compat_empty_size

## Location
[src/interfaces/ecpg/ecpglib/sqlda.c:45-64](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/sqlda.c#L45-L64)

## Overview
Calculates the minimum memory size required for an empty compatibility-mode SQLDA structure that can hold metadata for a given PostgreSQL result set.

## Definition

```c
structure and field structures */
	offset = sizeof(struct sqlda_compat) + sqld * sizeof(struct sqlvar_compat);
```
## Detailed Description
This function computes the base memory requirements for a compatibility-mode SQLDA structure before any actual data values are stored. It calculates space needed for the main SQLDA structure, all field descriptor structures, field names, and proper alignment padding. The "empty" designation means it only accounts for structural metadata, not the actual data values that would be stored later.

The calculation includes: the main sqlda_compat structure, an array of sqlvar_compat structures (one per field), storage for all field names as null-terminated strings, and alignment padding to ensure the first data field will be properly aligned.

## Parameters / Member Variables
- : Pointer to a PostgreSQL result set (PGresult) containing the query results and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [PQnfields](../P/PQnfields.md) (to get number of fields)
  - [PQfname](../P/PQfname.md) (to get field names)
  - [ecpg_sqlda_align_add_size](../e/ecpg_sqlda_align_add_size.md) (for alignment calculations)
  - [sqlda_compat](sqlda_compat.md) (structure type)
  - [sqlvar_compat](sqlvar_compat.md) (structure type)
- Called from (representative examples):
  - [sqlda_compat_total_size](sqlda_compat_total_size.md)
  - [ecpg_set_compat_sqlda](../e/ecpg_set_compat_sqlda.md)

## Notes and Other Information
This function is part of PostgreSQL's ECPG (Embedded SQL in C) interface, specifically for the compatibility-mode SQLDA implementation. The compatibility mode provides backward compatibility with older SQLDA interfaces. The function is essential for memory allocation planning before creating SQLDA structures that will hold query result metadata and data. The alignment padding ensures that subsequent data fields will be properly aligned for optimal memory access performance.

## Simplified Source

```c
static long sqlda_compat_empty_size(const PGresult *res) {
    int fieldCount = PQnfields(res);

    // Start with main structure + field descriptor array
    long offset = sizeof(struct sqlda_compat) +
                  fieldCount * sizeof(struct sqlvar_compat);

    // Add space for field names (null-terminated strings)
    for (int i = 0; i < fieldCount; i++) {
        offset += strlen(PQfname(res, i)) + 1;
    }

    // Add alignment padding for first data field
    ecpg_sqlda_align_add_size(offset, sizeof(int), 0, &offset, NULL);

    return offset;
}
```