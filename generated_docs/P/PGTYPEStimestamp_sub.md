# PGTYPEStimestamp_sub

## Location
[src/interfaces/ecpg/pgtypeslib/timestamp.c:797-809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/timestamp.c#L797-L809)

## Overview
Calculates the time difference between two PostgreSQL timestamps and returns the result as an interval.

## Definition
```c
int PGTYPEStimestamp_sub(timestamp *ts1, timestamp *ts2, interval *iv)
```

## Detailed Description
This function performs timestamp subtraction by computing the difference between two timestamp values and storing the result in an interval structure. The function handles infinite timestamp values by returning an error code. For finite timestamps, it calculates the time difference and stores it in the intervals time field, while setting the month component to zero since the result represents a pure time duration rather than a calendar-based interval.

## Parameters / Member Variables
- `ts1`: Pointer to the first timestamp (minuend)
- `ts2`: Pointer to the second timestamp (subtrahend) 
- `iv`: Pointer to the interval structure to store the result

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE
  - PGTYPES_TS_ERR_EINFTIME
- Called from (representative examples):
  - [dtsub](../d/dtsub.md) (in compatlib)

## Notes and Other Information
- Returns 0 on success, PGTYPES_TS_ERR_EINFTIME if either timestamp is infinite
- The resulting interval has month field set to 0, representing pure time duration
- Part of the ECPG pgtypes library for embedded SQL applications
- Used by compatibility layer functions for Informix-style operations

## Simplified Source

```c
int PGTYPEStimestamp_sub(timestamp *ts1, timestamp *ts2, interval *iv) {
    // Check for infinite timestamps
    if (TIMESTAMP_NOT_FINITE(*ts1) || TIMESTAMP_NOT_FINITE(*ts2))
        return PGTYPES_TS_ERR_EINFTIME;

    // Calculate time difference
    iv->time = (*ts1 - *ts2);
    iv->month = 0;  // Pure time duration, no month component

    return 0;
}
```