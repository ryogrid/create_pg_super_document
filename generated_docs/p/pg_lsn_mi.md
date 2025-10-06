# pg_lsn_mi

## Location
[src/backend/utils/adt/pg_lsn.c:224-250](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_lsn.c#L224-L250)

## Overview
The pg_lsn_mi function computes the signed difference between two PostgreSQL Log Sequence Numbers (LSNs), returning the result as a numeric value.

## Definition
```c
Datum pg_lsn_mi(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the subtraction operator for PostgreSQL's LSN data type. It takes two LSN values and computes their difference, handling the result as a signed 64-bit value. The function is designed to handle the full range of possible LSN differences, which can be as large as ±(2^63 - 1).

The implementation uses string formatting to convert the unsigned difference to a string representation, explicitly adding a minus sign for negative results when lsn1 < lsn2. The string is then converted to PostgreSQL's numeric type using the numeric_in function, which provides arbitrary precision arithmetic capabilities needed for such large values.

This function is crucial for WAL (Write-Ahead Logging) operations where calculating the distance between LSN positions is needed for replication lag monitoring, backup operations, and other database administration tasks.

## Parameters / Member Variables
The function uses PostgreSQL's standard function argument mechanism:
- Argument 0: First LSN value (lsn1) - retrieved using PG_GETARG_LSN(0)
- Argument 1: Second LSN value (lsn2) - retrieved using PG_GETARG_LSN(1)
- Internal variables:
  - `buf[256]`: Buffer for string formatting the numeric result
  - `result`: Datum containing the final numeric result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSN (macro for extracting LSN arguments)
  - UINT64_FORMAT (format string for 64-bit unsigned integers)
  - DirectFunctionCall3 (PostgreSQL function call mechanism)
  - [numeric_in](../n/numeric_in.md) (converts string to numeric type)
  - [CStringGetDatum](../C/CStringGetDatum.md) (converts C string to PostgreSQL Datum)
- Called from (representative examples):
  - [pg_wal_lsn_diff](pg_wal_lsn_diff.md)

## Notes and Other Information
- Part of PostgreSQL's arithmetic operators for LSN data type
- Handles the full range of 64-bit signed differences (±2^63 - 1)
- Uses PostgreSQL's numeric type to avoid overflow issues with large differences
- The result is always the absolute difference with appropriate sign handling
- Critical for WAL-related operations, replication monitoring, and backup management
- Located in src/backend/utils/adt/pg_lsn.c:224-250

## Simplified Source

```c
Datum pg_lsn_mi(PG_FUNCTION_ARGS) {
    // Extract the two LSN arguments
    XLogRecPtr lsn1 = PG_GETARG_LSN(0);
    XLogRecPtr lsn2 = PG_GETARG_LSN(1);
    char buf[256];

    // Calculate signed difference and format as string
    if (lsn1 < lsn2)
        snprintf(buf, sizeof buf, "-" UINT64_FORMAT, lsn2 - lsn1);
    else
        snprintf(buf, sizeof buf, UINT64_FORMAT, lsn1 - lsn2);

    // Convert string to numeric type for arbitrary precision
    Datum result = DirectFunctionCall3(numeric_in,
                                     CStringGetDatum(buf),
                                     ObjectIdGetDatum(0),
                                     Int32GetDatum(-1));
    return result;
}
```