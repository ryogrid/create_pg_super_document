# parseOidArray

## Location
[src/bin/pg_dump/common.c:1100-1146](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/common.c#L1100-L1146)

## Overview
Parses a space-delimited string of numbers into an array of Oid values, handling both Oids and potentially-signed attribute numbers.

## Definition
void parseOidArray(const char *str, Oid *array, int arraysize)

## Detailed Description
This function takes a string containing space-separated numeric values and converts them into an array of Oid values. It processes each character in the input string, accumulating digits (and minus signs) into temporary buffers, then converting complete numbers using atooid(). The function is designed to handle both positive Oids and potentially negative attribute numbers. If the input contains fewer numbers than the array size, remaining positions are filled with InvalidOid. Error handling includes validation for array bounds and invalid characters.

## Parameters / Member Variables
- `str`: Input string containing space-delimited numeric values to parse
- `array`: Output array of Oid values where parsed numbers will be stored
- `arraysize`: Maximum number of elements that can be stored in the array

## Dependencies
- Functions called/Symbols referenced:
  - atooid (converts string to Oid)
  - [pg_fatal](pg_fatal.md) (error reporting)
  - isdigit (character validation)
- Constants used:
  - InvalidOid
- Called from (representative examples):
  - [getAggregates](../g/getAggregates.md)
  - [getFuncs](../g/getFuncs.md)
  - [getIndexes](../g/getIndexes.md)
  - [dumpFunc](../d/dumpFunc.md)

## Notes and Other Information
- Uses a fixed-size temporary buffer (100 characters) for individual number parsing
- Accepts negative numbers (for attribute numbers) as well as positive Oids
- Fills unused array positions with InvalidOid
- Terminates fatally on parsing errors (too many numbers, invalid characters, numbers too long)
- Part of pg_dump's data parsing utilities for handling PostgreSQL system catalog output