# str_tolower_z

## Location
[src/backend/utils/adt/formatting.c:2235-2240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L2235-L2240)

## Overview
A convenience wrapper function that converts null-terminated strings to lowercase using PostgreSQL's locale-aware string conversion functionality.

## Definition
static char *str_tolower_z(const char *buff, Oid collid)

## Detailed Description
The str_tolower_z function serves as a convenience wrapper around the more general str_tolower function for cases where the input string is null-terminated. Instead of requiring the caller to explicitly provide the string length, this function automatically calculates the length using strlen() and then delegates to str_tolower for the actual conversion work.

This function is part of PostgreSQL's internal formatting utilities and provides locale-aware lowercase conversion based on the specified collation identifier. The 'z' suffix conventionally indicates that the function expects null-terminated (zero-terminated) strings.

## Parameters / Member Variables
- : Input null-terminated string to convert to lowercase
- : Collation identifier (Oid) to use for locale-aware conversion

## Dependencies
- Functions called/Symbols referenced:
  - [str_tolower](str_tolower.md)
- Called from (representative examples):
  - (No direct references found in current analysis)

## Notes and Other Information
- Static function, only available within the formatting.c compilation unit
- Automatically calculates string length using strlen(), making it convenient for null-terminated strings
- Delegates actual conversion logic to str_tolower function
- Part of a family of convenience functions for string case conversion in PostgreSQL's formatting system
- Supports collation-aware conversion for proper locale handling