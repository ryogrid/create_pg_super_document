# dtsub

## Location
[src/interfaces/ecpg/compatlib/informix.c:650-655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L650-L655)

## Overview
The dtsub function calculates the difference between two timestamps and returns the result as an interval, providing Informix ESQL/C compatibility for timestamp arithmetic operations.

## Definition
```c
int dtsub(timestamp * ts1, timestamp * ts2, interval * iv)
```

## Detailed Description
This function performs timestamp subtraction by calculating the time difference between two timestamp values and storing the result in an interval structure. It serves as a compatibility wrapper for Informix applications that need to perform date/time arithmetic operations. The function calls PostgreSQL's PGTYPEStimestamp_sub() internally to perform the actual calculation.

The subtraction operation computes ts1 - ts2, meaning ts2 is subtracted from ts1. The result represents the duration between the two timestamps. The function handles special cases involving infinite timestamps and returns appropriate error codes for invalid operations.

## Parameters / Member Variables
- `ts1`: Pointer to the first timestamp (minuend) from which the second timestamp will be subtracted
- `ts2`: Pointer to the second timestamp (subtrahend) to be subtracted from the first
- `iv`: Pointer to an interval structure where the difference will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPEStimestamp_sub](../P/PGTYPEStimestamp_sub.md)
  - interval (data type)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:650-655
- Returns 0 on success, or an error code (e.g., PGTYPES_TS_ERR_EINFTIME for infinite timestamps)
- Part of the Informix compatibility layer in PostgreSQL ECPG
- The result interval contains only time components; month components are set to 0
- Handles special timestamp values like positive and negative infinity
- The operation is equivalent to the SQL expression `ts1 - ts2`
- Provides date/time arithmetic capabilities for embedded SQL applications

## Simplified Source
```c
int
dtsub(timestamp *ts1, timestamp *ts2, interval *iv)
{
    // Calculate ts1 - ts2 and store in interval
    return PGTYPEStimestamp_sub(ts1, ts2, iv);
}
```