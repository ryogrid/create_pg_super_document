# rdefmtdate

## Location
[src/interfaces/ecpg/compatlib/informix.c:553-578](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L553-L578)

## Overview
Parses a date string according to a specified format and stores the result in a date variable, with Informix-compatible error handling.

## Definition

```c
int
rdefmtdate(date * d, const char *fmt, const char *str)
```
## Detailed Description
The  function is part of PostgreSQL's ECPG Informix compatibility library. It parses a date string () according to the specified format () and stores the resulting date value in the provided date pointer (). The function acts as a wrapper around PostgreSQL's internal  function, translating PostgreSQL-specific error codes to Informix-compatible error codes.

The function includes a TODO comment indicating that it should handle the DBCENTURY environment variable, though currently PostgreSQL functions allow all centuries. Error handling maps various PostgreSQL date parsing errors to their Informix equivalents for compatibility.

## Parameters / Member Variables
- `*d`: Pointer to a date variable where the parsed date will be stored
- `*fmt`: Format string specifying how to interpret the input date string
- `*str`: Input date string to be parsed
## Dependencies
- Functions called/Symbols referenced:
  - : Internal PostgreSQL function for parsing date strings
  - : PostgreSQL error code for short date format
  - : PostgreSQL error code for invalid arguments
  - : PostgreSQL error code for invalid date format
  - : PostgreSQL error code for invalid day
  - : PostgreSQL error code for invalid month
  - : Informix-compatible error code for short date
  - : Informix-compatible error code for invalid format
  - : Informix-compatible error code for invalid day
  - : Informix-compatible error code for invalid month
  - : Informix-compatible error code for invalid year
- Called from (representative examples):
  - : Related date parsing function in the same file
  - : Test function in the ECPG test suite
  - Referenced in  macro

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:553-578
- Returns 0 on success, or an Informix-compatible error code on failure
- Contains a TODO to handle the DBCENTURY environment variable for century handling
- Provides comprehensive error mapping between PostgreSQL and Informix error codes
- Part of the ECPG embedded SQL interface for maintaining Informix application compatibility