# asc_toupper

## Location
src/backend/utils/adt/formatting.c: 2181 - 2203

## Overview
A utility function that converts ASCII characters in a string to uppercase, designed specifically for formatting operations in PostgreSQL.

## Definition
char *asc_toupper(const char *buff, size_t nbytes)

## Detailed Description
The asc_toupper function provides ASCII-only uppercase conversion functionality for PostgreSQL's formatting system. It takes a buffer of characters and a byte count, then creates a new null-terminated string with all ASCII lowercase letters converted to uppercase. The function is optimized for performance by operating directly on ASCII characters without locale considerations, making it suitable for internal PostgreSQL formatting operations where consistent behavior across different locales is required.

The function handles memory management by allocating a new string using pnstrdup and returns a palloc'd result that needs to be freed by the caller. It gracefully handles NULL input by returning NULL.

## Parameters / Member Variables
- : Input character buffer to convert (can be NULL)
- : Number of bytes to process from the input buffer

## Dependencies
- Functions called/Symbols referenced:
  - pnstrdup
  - pg_ascii_toupper
- Called from (representative examples):
  - str_toupper
  - asc_toupper_z

## Notes and Other Information
- Returns a palloc'd string that must be freed by the caller
- Only converts ASCII characters (a-z to A-Z), leaving other characters unchanged
- Designed for use in PostgreSQL's formatting system where locale-independent behavior is desired
- Part of the formatting.c module which handles various string formatting operations