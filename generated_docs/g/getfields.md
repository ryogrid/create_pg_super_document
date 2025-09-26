# getfields

## Location
[src/timezone/zic.c:3717-3756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L3717-L3756)

## Overview
A string parsing function that splits input lines into whitespace-separated fields while handling quoted strings and comments, primarily used for processing timezone rule files.

## Definition
static char **getfields(char *cp)

## Detailed Description
The getfields function parses a line of input text and splits it into an array of string fields. It implements sophisticated parsing logic that handles:

1. **Whitespace separation**: Fields are separated by whitespace characters (spaces, tabs, etc.)
2. **Quote handling**: Strings enclosed in double quotes are treated as single fields, with the quotes removed
3. **Comment processing**: Everything after a '#' character is treated as a comment and ignored
4. **Memory management**: Returns a dynamically allocated array of string pointers

The function processes the input character by character, building fields in-place by modifying the original string and creating an array of pointers to the start of each field. Quoted strings can contain whitespace and are processed by copying characters until the closing quote is found.

The parsing stops when either the end of string is reached or a comment character ('#') is encountered. The resulting array is NULL-terminated for easy iteration.

## Parameters / Member Variables
- : Input string to be parsed into fields (modified in-place during processing)

## Dependencies
- Functions called/Symbols referenced:
  - [emalloc](../e/emalloc.md) (memory allocation wrapper)
  - [size_product](../s/size_product.md) (safe multiplication for memory allocation)
  - [is_space](../i/is_space.md) (whitespace character checking)
  - [error](../e/error.md) (error reporting function)
  - EXIT_FAILURE (exit status constant)
- Called from (representative examples):
  - [infile](../i/infile.md)

## Notes and Other Information
- Returns NULL if input string is NULL
- Modifies the input string in-place by inserting null terminators
- Allocates memory for the result array based on input string length
- Exits the program with EXIT_FAILURE if unmatched quotation marks are found
- The returned array is NULL-terminated to indicate the end of fields
- Part of the timezone compiler (zic) infrastructure for parsing configuration files
- Handles both simple whitespace-delimited tokens and complex quoted strings containing spaces
- Comments (lines starting with '#') cause parsing to stop, effectively ignoring the rest of the line