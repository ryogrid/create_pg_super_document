# PGTYPEStimestamp_sub

## Location
src/interfaces/ecpg/pgtypeslib/timestamp.c: 797 - 809

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
  - dtsub (in compatlib)

## Notes and Other Information
- Returns 0 on success, PGTYPES_TS_ERR_EINFTIME if either timestamp is infinite
- The resulting interval has month field set to 0, representing pure time duration
- Part of the ECPG pgtypes library for embedded SQL applications
- Used by compatibility layer functions for Informix-style operations