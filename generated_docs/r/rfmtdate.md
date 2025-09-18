# rfmtdate

## Location
[src/interfaces/ecpg/compatlib/informix.c:579-591](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L579-L591)

## Overview
Formats a date value into a string according to a specified format, providing Informix-compatible error handling.

## Definition


## Detailed Description
The  function is part of PostgreSQL's ECPG Informix compatibility library. It takes a date value and formats it into a string representation according to the specified format string. The function serves as a wrapper around PostgreSQL's internal  function, translating system errors to Informix-compatible error codes.

The function handles memory allocation errors specifically by checking for  errno and returns appropriate Informix error codes. For other formatting errors, it returns a general date conversion error code.

## Parameters / Member Variables
- : Input date value to be formatted
- : Format string specifying the desired output format
- : Output buffer where the formatted date string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - : Internal PostgreSQL function for formatting dates to strings
  - : Informix-compatible error code for memory allocation failures
  - : Informix-compatible error code for general date conversion errors
- Called from (representative examples):
  - : Test function in the ECPG test suite
  - Referenced in  macro

## Notes and Other Information
- Located in src/interfaces/ecpg/compatlib/informix.c:579-591
- Returns 0 on success, or an Informix-compatible error code on failure
- Specifically handles memory allocation errors by checking errno for ENOMEM
- Part of the ECPG embedded SQL interface for maintaining Informix application compatibility
- Complements  by providing the inverse operation (formatting vs. parsing)
- The output string buffer must be allocated by the caller with sufficient space