# parse_one_reloption

## Location
[src/backend/access/common/reloptions.c:1578-1710](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1578-L1710)

## Overview
Static function that parses and validates a single relation option's value, handling type-specific parsing and validation for different option types (bool, int, real, enum, string).

## Definition
```c
static void parse_one_reloption(relopt_value *option, char *text_str, int text_len, bool validate)
```

## Detailed Description
This function is the core parsing logic for individual relation options, called by parseRelOptionsInternal() for each "key=value" pair. It extracts the value portion from the text string, performs type-specific parsing based on the option's defined type (RELOPT_TYPE_BOOL, RELOPT_TYPE_INT, RELOPT_TYPE_REAL, RELOPT_TYPE_ENUM, RELOPT_TYPE_STRING), and validates the parsed value against type-specific constraints. The function handles duplicate option detection, range checking for numeric types, enum value validation, and optional string validation callbacks. Upon successful parsing, it marks the option as set (isset=true) and stores the parsed value in the appropriate union member.

## Parameters / Member Variables
- `option`: Pointer to relopt_value structure to populate with parsed value
- `text_str`: Raw text string containing "optionname=value" format
- `text_len`: Length of the text_str parameter
- `validate`: Boolean flag enabling validation and error reporting

## Dependencies
- Functions called/Symbols referenced:
  - ereport
  - [palloc](palloc.md)
  - memcpy
  - [parse_bool](parse_bool.md)
  - [parse_int](parse_int.md)
  - [parse_real](parse_real.md)
  - [pg_strcasecmp](pg_strcasecmp.md)
  - [errdetail_internal](../e/errdetail_internal.md)
  - elog
  - [pfree](pfree.md)
- Called from (representative examples):
  - [parseRelOptionsInternal](parseRelOptionsInternal.md)

## Notes and Other Information
- Detects and reports duplicate option specifications when validation is enabled
- Performs bounds checking for integer and real number types against min/max constraints
- Supports case-insensitive enum value matching using pg_strcasecmp
- [String](../S/String.md) values are stored directly without copying (nofree=true) for efficiency
- Provides detailed error messages including valid value ranges for out-of-bounds errors
- Uses PostgreSQL's standard error reporting system with appropriate error codes
- Memory management includes conditional freeing based on option type (strings are not freed)