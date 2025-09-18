# parseRelOptionsInternal

## Location
[src/backend/access/common/reloptions.c:1436-1507](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1436-L1507)

## Overview
Internal function that parses relation options from a Datum array and populates a pre-allocated reloptions array with parsed values.

## Definition
```c
static void parseRelOptionsInternal(Datum options, bool validate, relopt_value *reloptions, int numoptions)
```

## Detailed Description
This static function serves as the core parsing engine for relation options in PostgreSQL. It takes a Datum containing an array of text options (in "key=value" format) and parses them into a pre-allocated array of relopt_value structures. The function deconstructs the input array, iterates through each option string, matches it against known relation options, and delegates the actual parsing of individual options to parse_one_reloption(). If validation is enabled and an unrecognized parameter is encountered, it raises an error.

## Parameters / Member Variables
- `options`: Datum containing an ArrayType of text strings representing relation options
- `validate`: Boolean flag indicating whether to validate option names and raise errors for unrecognized parameters
- `reloptions`: Pre-allocated array of relopt_value structures to be populated with parsed values
- `numoptions`: Number of elements in the reloptions array

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetArrayTypeP
  - [deconstruct_array_builtin](../d/deconstruct_array_builtin.md)
  - VARDATA
  - VARSIZE
  - [parse_one_reloption](parse_one_reloption.md)
  - TextDatumGetCString
  - strchr
  - strncmp
  - ereport
  - [pfree](pfree.md)
- Called from (representative examples):
  - [parseRelOptions](parseRelOptions.md)
  - [parseLocalRelOptions](parseLocalRelOptions.md)

## Notes and Other Information
- This is a static function, only accessible within the reloptions.c file
- Performs memory management by freeing allocated arrays to avoid memory leaks
- Uses string matching with strncmp to identify option names
- Error handling includes specific error codes (ERRCODE_INVALID_PARAMETER_VALUE) for invalid parameters
- The function expects options in "key=value" format and will truncate the key name at the '=' for error reporting