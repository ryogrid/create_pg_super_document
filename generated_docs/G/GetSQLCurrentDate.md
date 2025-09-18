# GetSQLCurrentDate

## Location
src/backend/utils/adt/date.c: 309 - 341

## Overview  
Implements the SQL CURRENT_DATE function by returning the current date as a PostgreSQL DateADT value with caching optimization.

## Definition
```c
DateADT GetSQLCurrentDate(void)
```

## Detailed Description
The `GetSQLCurrentDate` function implements PostgreSQL's SQL CURRENT_DATE functionality, returning the current date in the local timezone as a DateADT value. The function includes an intelligent caching mechanism to avoid expensive Julian day calculations when called multiple times within the same day. It maintains static variables to cache the last computed date components and the resulting DateADT value, only recalculating when the year, month, or day has changed. This optimization is particularly valuable since the date2j function involves several integer divisions, and most database sessions don't span across midnight.

## Parameters / Member Variables  
- `tm`: pg_tm structure to hold the current date/time components
- `cache_year`: Static variable caching the last computed year
- `cache_mon`: Static variable caching the last computed month  
- `cache_mday`: Static variable caching the last computed day
- `cache_date`: Static variable caching the last computed DateADT result

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentDateTime: Retrieves current date and time into pg_tm structure
  - date2j: Converts year/month/day to Julian day number
  - POSTGRES_EPOCH_JDATE: Constant representing PostgreSQL's epoch in Julian days
- Called from (representative examples):
  - ExecEvalSQLValueFunction: Executor function for evaluating SQL value functions
  - PG_RETURN_TIMETZADT_P: Related time zone date handling functions

## Notes and Other Information
- This function directly implements the SQL CURRENT_DATE standard function
- The caching optimization assumes most database sessions don't span across midnight
- Returns the date in the local timezone, not UTC
- Cache invalidation occurs when any date component (year, month, day) changes
- The function is thread-safe as long as GetCurrentDateTime is thread-safe
- Used internally by PostgreSQL's SQL executor when CURRENT_DATE is referenced in queries
- The optimization significantly reduces computational overhead for applications that frequently access the current date