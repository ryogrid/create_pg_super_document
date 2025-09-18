# timestamp_in

## Location
src/backend/utils/adt/timestamp.c: 164 - 231

## Overview
A PostgreSQL input function that converts string representations of timestamps into internal timestamp format (without timezone), handling various input formats and special values.

## Definition


## Detailed Description
This function implements the input conversion for the TIMESTAMP data type (without timezone). It parses string representations of timestamps and converts them to PostgreSQL's internal timestamp format. The function handles a wide variety of input formats including ISO 8601, SQL standard formats, and PostgreSQL-specific special values like 'epoch', 'infinity', and '-infinity'.

The parsing process involves multiple stages: first tokenizing the input string using ParseDateTime, then interpreting the tokens with DecodeDateTime, and finally converting the parsed components into PostgreSQL's internal timestamp representation. The function also applies type modifier constraints (precision) and performs range validation.

## Parameters / Member Variables
- Function follows PostgreSQL's fmgr calling convention (PG_FUNCTION_ARGS)
-  (arg 0): Input string to be parsed as a timestamp
-  (arg 1): Type element OID (unused in current implementation)
-  (arg 2): Type modifier specifying precision constraints
- : Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - ParseDateTime: Initial string parsing and tokenization
  - DecodeDateTime: Token interpretation and datetime component extraction
  - DateTimeParseError: Error reporting for parsing failures
  - tm2timestamp: Conversion from broken-down time to timestamp
  - SetEpochTimestamp: Handling of 'epoch' special value
  - AdjustTimestampForTypmod: Applying precision constraints
  - TIMESTAMP_NOEND/TIMESTAMP_NOBEGIN: Handling infinity values
  - PG_RETURN_TIMESTAMP: Return value macro
- Called from: Used as input function for TIMESTAMP type (registered in pg_type catalog)

## Notes and Other Information
- Supports special values: 'epoch' (1970-01-01 00:00:00), 'infinity', '-infinity'
- Handles various datetime formats through PostgreSQL's flexible parsing engine
- Performs range checking and reports appropriate error codes for out-of-range values
- Uses soft error handling (escontext) to allow callers to handle errors gracefully
- The typmod parameter controls fractional seconds precision (0-6 digits)
- Input parsing is locale-aware and respects DateStyle settings
- Returns NULL on parsing errors when operating in soft error mode