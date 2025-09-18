# str_initcap_z

## Location
src/backend/utils/adt/formatting.c: 2247 - 2252

## Overview
A convenience wrapper function that converts null-terminated strings to initial capital format using PostgreSQL's locale-aware string conversion functionality.

## Definition
static char *str_initcap_z(const char *buff, Oid collid)

## Detailed Description
The str_initcap_z function serves as a convenience wrapper around the more general str_initcap function for cases where the input string is null-terminated. Instead of requiring the caller to explicitly provide the string length, this function automatically calculates the length using strlen() and then delegates to str_initcap for the actual conversion work.

This function is part of PostgreSQL's internal formatting utilities and provides locale-aware initial capitalization (first letter of each word capitalized) based on the specified collation identifier. The 'z' suffix conventionally indicates that the function expects null-terminated (zero-terminated) strings, making it easier to use with standard C strings.

## Parameters / Member Variables
- : Input null-terminated string to convert to initial capital format
- : Collation identifier (Oid) to use for locale-aware conversion

## Dependencies
- Functions called/Symbols referenced:
  - str_initcap
- Called from (representative examples):
  - (No direct references found in current analysis)

## Notes and Other Information
- Static function, only available within the formatting.c compilation unit
- Automatically calculates string length using strlen(), making it convenient for null-terminated strings
- Delegates actual conversion logic to str_initcap function
- Part of a family of convenience functions for string case conversion in PostgreSQL's formatting system
- Supports collation-aware conversion for proper locale handling
- Completes the trio of null-terminated string convenience wrappers along with str_tolower_z and str_toupper_z