# int8in

## Location
src/backend/utils/adt/int8.c: 50 - 60

## Overview
Converts a string representation of a 64-bit integer to PostgreSQL's internal int8 (bigint) format.

## Definition


## Detailed Description
The  function serves as the input conversion routine for PostgreSQL's int8 data type (also known as bigint). It takes a C-string representation of a 64-bit integer and converts it to PostgreSQL's internal Datum representation. This function is part of the PostgreSQL type system's input/output infrastructure and is automatically called when converting text input to int8 values during SQL parsing and execution.

The function utilizes  to perform the actual string-to-integer conversion with proper error handling and overflow detection.

## Parameters / Member Variables
- : Standard PostgreSQL function calling convention macro that provides access to:
  -  (char*): C-string containing the text representation of the integer to convert

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract C-string argument
  - : Safe string-to-int64 conversion function with error handling
  - : Macro to return int64 value as Datum
- Called from (representative examples):
  - : Used in command definition parsing
  - : Used in JSON path execution

## Notes and Other Information
- This function is registered in the PostgreSQL type system as the input function for the int8/bigint data type
- Error handling for invalid input strings is delegated to 
- The function follows PostgreSQL's standard input function pattern using the PG_FUNCTION_ARGS calling convention
- Located in src/backend/utils/adt/int8.c, which contains all arithmetic and conversion routines for 64-bit integers