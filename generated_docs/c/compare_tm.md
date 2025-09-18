# compare_tm

## Location
src/bin/initdb/findtimezone.c: 207 - 233

## Overview
Compares a system tm structure with a PostgreSQL-specific pg_tm structure to determine if they represent the same time values.

## Definition


## Detailed Description
This function performs a field-by-field comparison between a standard C library  and PostgreSQL's  to verify if they contain identical time information. It checks all relevant time components including seconds, minutes, hours, day, month, year, day of week, day of year, and daylight saving time flag. The function is used internally during timezone detection and validation processes to ensure that PostgreSQL's time calculations match the system's time calculations.

## Parameters / Member Variables
- : Pointer to a standard C library  containing system time information
- : Pointer to a PostgreSQL-specific  containing PostgreSQL's calculated time information

## Dependencies
- Functions called/Symbols referenced:
  - pg_tm (PostgreSQL time structure type)
- Called from (representative examples):
  - score_timezone

## Notes and Other Information
- This function is marked as , indicating it's only used within the findtimezone.c file
- Returns  if all time fields match exactly,  if any field differs
- Part of the timezone detection mechanism in initdb
- Used to validate that PostgreSQL's timezone calculations align with the system's timezone behavior