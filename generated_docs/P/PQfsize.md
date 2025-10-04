# PQfsize

## Location
[src/interfaces/libpq/fe-exec.c:3730-3740](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3730-L3740)

## Overview
Returns the size in bytes of the specified field's PostgreSQL data type.

## Definition
int PQfsize(const PGresult *res, int field_num)

## Detailed Description
PQfsize retrieves the size in bytes of the PostgreSQL data type for the specified field in a query result. This function is part of PostgreSQL's libpq client library and provides essential size information for data type handling. The returned value represents the internal storage size of the data type as defined in PostgreSQL's type system. For fixed-length types (like INTEGER, BIGINT, TIMESTAMP), this returns the actual byte size. For variable-length types (like TEXT, VARCHAR, BYTEA), this typically returns -1 to indicate variable length. Understanding type sizes is crucial for applications that need to allocate appropriate memory buffers, perform binary data processing, or interface with external systems that require explicit size information.

## Parameters / Member Variables
- res: Pointer to a PGresult structure containing the query result
- field_num: Zero-based index of the field (column) for which to retrieve the type size

## Dependencies
- Functions called/Symbols referenced:
  - [check_field_number](../c/check_field_number.md): Validates that field_num is within valid range
- Called from (representative examples):
  - [ECPGget_desc](../E/ECPGget_desc.md) (ECPG): Size information for embedded SQL descriptor handling
  - [ecpg_build_compat_sqlda](../e/ecpg_build_compat_sqlda.md) (ECPG): SQLDA structure building with size information

## Notes and Other Information
- Returns 0 if the field number is out of range or if attribute information is not available
- The function accesses the typlen member of the PGresAttDesc structure stored in res->attDescs
- Fixed-length types return their byte size (e.g., 4 for INTEGER, 8 for BIGINT)
- [Variable](../V/Variable.md)-length types typically return -1 (e.g., TEXT, VARCHAR, BYTEA)
- Special value -2 indicates a null-terminated C string (CSTRING type)
- This function is thread-safe as it only reads from the PGresult structure
- Type size information is determined by PostgreSQL's internal type definitions
- For variable-length types, use PQgetlength() to get the actual length of specific values
- Essential for binary format processing and memory management in client applications
- Defined in src/interfaces/libpq/fe-exec.c:3730-3740

## Simplified Source

```c
int PQfsize(const PGresult *res, int field_num)
{
    // Validate field number is in range
    if (!check_field_number(res, field_num))
        return 0;

    // Return type length if attribute descriptors available
    if (res->attDescs)
        return res->attDescs[field_num].typlen;
    else
        return 0;
}
```