# str_toupper_z

## Location
src/backend/utils/adt/formatting.c: 2241 - 2246

## Overview
A convenience wrapper function that converts null-terminated strings to uppercase using PostgreSQL's locale-aware string conversion functionality.

## Definition
static char *str_toupper_z(const char *buff, Oid collid)

## Detailed Description
The str_toupper_z function serves as a convenience wrapper around the more general str_toupper function for cases where the input string is null-terminated. Instead of requiring the caller to explicitly provide the string length, this function automatically calculates the length using strlen() and then delegates to str_toupper for the actual conversion work.

This function is part of PostgreSQL's internal formatting utilities and provides locale-aware uppercase conversion based on the specified collation identifier. The 'z' suffix conventionally indicates that the function expects null-terminated (zero-terminated) strings, making it easier to use with standard C strings.

## Parameters / Member Variables
- : Input null-terminated string to convert to uppercase
- : Collation identifier (Oid) to use for locale-aware conversion

## Dependencies
- Functions called/Symbols referenced:
  - str_toupper
- Called from (representative examples):
  - (No direct references found in current analysis)

## Notes and Other Information
- Static function, only available within the formatting.c compilation unit
- Automatically calculates string length using strlen(), making it convenient for null-terminated strings
- Delegates actual conversion logic to str_toupper function
- Part of a family of convenience functions for string case conversion in PostgreSQL's formatting system
- Supports collation-aware conversion for proper locale handling
- Companion function to str_tolower_z, providing uppercase conversion functionality