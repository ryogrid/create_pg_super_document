# replace_token

## Location
[src/bin/initdb/initdb.c:470-524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L470-L524)

## Overview
Modifies an array of strings by replacing the first occurrence of a specified token with a replacement string on each line.

## Definition


## Detailed Description
This function performs string replacement operations on an array of malloc'd strings, similar to basic sed functionality but without requiring regular expressions. It searches for the first occurrence of a token string in each line of the array and replaces it with the specified replacement string. The function handles memory management by freeing old strings and allocating new ones when replacements change the string length. This is primarily used during PostgreSQL database initialization to customize configuration templates.

## Parameters / Member Variables
- : Array of malloc'd strings to be processed, terminated by NULL pointer
- : The string to search for and replace in each line
- : The string to replace the token with

## Dependencies
- Functions called/Symbols referenced:
  -  (standard library function)
  -  (standard library function) 
  -  (PostgreSQL memory allocation wrapper)
  -  (standard library function)
  -  (standard library function)
  -                total        used        free      shared  buff/cache   available
Mem:        32819380     4786744    25565284        3040     2467352    27650412
Swap:        8388608           0     8388608 (standard library function)
- Called from (representative examples):
  -  (multiple times for various configuration replacements)
  -  (multiple times for template database setup)
  - Used with  macro

## Notes and Other Information
- The function modifies the input array in-place, freeing original strings when replacements occur
- Only replaces the first occurrence of the token on each line
- Efficiently handles size differences between token and replacement strings
- Part of initdb utility's template processing system
- Designed to avoid dependencies on regular expression libraries for simple text substitution