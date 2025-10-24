# PQbinaryTuples

## Location
[src/interfaces/libpq/fe-exec.c:3497-3509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L3497-L3509)

## Overview
PQbinaryTuples determines whether a PGresult contains data in binary format rather than text format.

## Definition
int PQbinaryTuples(const PGresult *res)

## Detailed Description
This function checks the format of data stored in a PGresult object and returns whether the result contains binary-formatted data. PostgreSQL can return query results in either text format (human-readable strings) or binary format (native data type representations). Binary format is more efficient for data transfer and processing but requires the client application to handle the binary data appropriately.

The function returns a boolean-style integer value indicating the format type. When PostgreSQL executes a query with binary result format requested, all columns in the result set use binary format. The format is determined at query execution time and applies uniformly to the entire result set.

## Parameters / Member Variables
- : Pointer to the PGresult object to examine

## Dependencies
- Functions called/Symbols referenced:
  - None (accesses res->binary directly)
- Called from (representative examples):
  - [HandleCopyResult](../H/HandleCopyResult.md) (psql copy operation handling)

## Notes and Other Information
- Returns 0 if the PGresult pointer is NULL
- Returns 1 if the result set contains binary-formatted data
- Returns 0 if the result set contains text-formatted data
- Binary format is more efficient for network transfer and parsing
- Binary format requires client-side knowledge of PostgreSQL's internal data representations
- The format applies to all columns in the result set uniformly
- Binary format is typically used by sophisticated client applications and drivers
- Most simple applications use text format for easier debugging and development
- Part of the libpq result format inspection API
- Binary format handling requires careful attention to byte order and data type specifics

## Simplified Source

```c
int PQbinaryTuples(const PGresult *res) {
    // Return 0 for NULL result
    if (!res)
        return 0;

    // Return whether the result uses binary format
    return res->binary;
}
```