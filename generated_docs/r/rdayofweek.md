# rdayofweek

## Location
[src/interfaces/ecpg/compatlib/informix.c:604-611](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L604-L611)

## Overview
Returns the day of the week for a given date value in the ECPG Informix compatibility library.

## Definition

```c
int
rdayofweek(date d)
```
## Detailed Description
The  function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility layer. It takes a Julian date value and returns the corresponding day of the week as an integer. This function serves as a simple wrapper around PostgreSQL's internal  function, providing Informix-compatible access to day-of-week calculations.

The function directly delegates the calculation to PostgreSQL's internal date handling functions without any additional processing or error handling, making it a straightforward pass-through function for compatibility purposes.

## Parameters / Member Variables
- : Input date value for which to determine the day of the week

## Dependencies
- Functions called/Symbols referenced:
  - : Internal PostgreSQL function that calculates the day of the week for a given date
  - Thu Sep 11 03:48:32 JST 2025: Date type used for the input parameter
- Called from (representative examples):
  - Referenced in  macro in ecpg_informix.h

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:604-611
- Returns an integer representing the day of the week (typically 0-6 or 1-7 depending on the system convention)
- This is the simplest function in the date conversion family, requiring no data type conversion or error handling
- Part of the ECPG embedded SQL interface for maintaining Informix application compatibility
- Provides a direct interface to PostgreSQL's internal day-of-week calculation functionality
- No error handling is performed since the underlying PostgreSQL function handles date validation