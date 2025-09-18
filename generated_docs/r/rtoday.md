# rtoday

## Location
[src/interfaces/ecpg/compatlib/informix.c:535-540](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L535-L540)

## Overview
Sets a date variable to the current date, providing Informix compatibility functionality in PostgreSQL's ECPG interface.

## Definition
```c
void rtoday(date *d)
```

## Detailed Description
The `rtoday` function is part of PostgreSQL's ECPG date handling compatibility library for Informix. It provides a simple wrapper around PostgreSQL's `PGTYPESdate_today` function to set a date variable to the current system date. This function is commonly used in Informix applications to initialize date variables with "today's" date and maintains compatibility with existing Informix code patterns. The function directly delegates to PostgreSQL's native date handling routines.

## Parameters / Member Variables
- `d`: Pointer to the date variable that will be set to the current date

## Dependencies
- Functions called/Symbols referenced:
  - [PGTYPESdate_today](../P/PGTYPESdate_today.md)
  - date (type)
- Called from (representative examples):
  - ECPG_INFORMIX_EXTRA_CHARS (referenced in header)

## Notes and Other Information
- Returns void (no error checking required as it uses system date)
- Simple wrapper around PostgreSQL's native date functionality
- Part of the Informix compatibility layer in PostgreSQL ECPG
- Located in src/interfaces/ecpg/compatlib/informix.c:535-540
- Minimal implementation - just a direct call to `PGTYPESdate_today`
- Commonly used for initializing date variables in business applications
- Uses system date/time to determine the current date value