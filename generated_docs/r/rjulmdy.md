# rjulmdy

## Location
[src/interfaces/ecpg/compatlib/informix.c:541-552](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L541-L552)

## Overview
Converts a Julian date to month, day, and year components in the ECPG Informix compatibility library.

## Definition

```c
int
rjulmdy(date d, short *mdy)
```
## Detailed Description
The  function is part of PostgreSQL's ECPG (Embedded SQL in C) Informix compatibility layer. It takes a Julian date value and converts it into separate month, day, and year components stored in a short integer array. This function serves as a wrapper around the internal  function, converting the integer results to short integers for Informix compatibility.

The function always returns 0, indicating successful conversion. The actual conversion logic is delegated to the PostgreSQL internal date handling functions.

## Parameters / Member Variables
- : Input Julian date value of type Thu Sep 11 03:46:03 JST 2025
- : Output array of 3 short integers where:
  - : Month component
  - : Day component  
  - : Year component

## Dependencies
- Functions called/Symbols referenced:
  - : Internal PostgreSQL function that performs the actual Julian date to MDY conversion
  - Thu Sep 11 03:46:04 JST 2025: Date type used for the input parameter
- Called from (representative examples):
  - Referenced in  macro in ecpg_informix.h

## Notes and Other Information
- This function is specifically designed for Informix compatibility in the ECPG interface
- Located in the compatibility library at src/interfaces/ecpg/compatlib/informix.c:541-552
- The function converts integer results from the internal PostgreSQL date function to short integers to match Informix's expected data types
- Always returns 0 (success) - [error](../e/error.md) handling is presumably done by the underlying  function

## Simplified Source

```c
int rjulmdy(date d, short *mdy) {
    // Convert Julian date to month/day/year using PostgreSQL's built-in function
    int mdy_int[3];
    PGTYPESdate_julmdy(d, mdy_int);

    // Convert int results to short for Informix compatibility
    mdy[0] = (short) mdy_int[0];  // Month
    mdy[1] = (short) mdy_int[1];  // Day
    mdy[2] = (short) mdy_int[2];  // Year

    return 0;  // Always success
}
```