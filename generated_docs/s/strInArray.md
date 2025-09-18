# strInArray

## Location
src/bin/pg_dump/common.c: 1147 - 1157

## Overview
Searches for a string pattern within an array of strings and returns the index if found, or -1 if not found.

## Definition
static int strInArray(const char *pattern, char **arr, int arr_size)

## Detailed Description
This is a simple utility function that performs linear search through an array of strings to find a match for a given pattern. It uses strcmp() for exact string comparison and returns the zero-based index of the first matching element. If no match is found, it returns -1. The function is declared static, making it internal to the common.c module in pg_dump.

## Parameters / Member Variables
- `pattern`: The string to search for in the array
- `arr`: Array of string pointers to search through
- `arr_size`: Number of elements in the string array

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library function for string comparison)
- Called from (representative examples):
  - flagInhAttrs
  - Referenced near CATALOGIDHASH_INITIAL_SIZE

## Notes and Other Information
- Performs case-sensitive exact string matching using strcmp()
- Returns the first matching index (0-based) or -1 if no match found
- Linear search algorithm with O(n) time complexity
- Static function scope limits usage to within the common.c source file
- Part of pg_dump's internal utility functions for string processing