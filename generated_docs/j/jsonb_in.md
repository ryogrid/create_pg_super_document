# jsonb_in

## Location
[src/backend/utils/adt/jsonb.c:73-88](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L73-L88)

## Overview
The  function is the input function for the JSONB data type, responsible for converting string representations of JSON into PostgreSQL's internal JSONB format.

## Definition

```c
Datum
jsonb_in(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the primary entry point for converting JSON text strings into JSONB values. It is automatically called by PostgreSQL's type system when converting string literals or text values to JSONB type. The function extracts the input string parameter and delegates the actual parsing and conversion work to the  function with appropriate parameters for standard input processing.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (char*): The input JSON string to be parsed and converted to JSONB format

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that performs the actual JSON parsing and JSONB conversion
  - : Macro to extract string argument from function call
  - : Standard C library function to determine string length
- Called from (representative examples):
  - : Expression evaluation for JSON coercion operations
  - : Parser transformation of JSON behavior clauses
  - : Retrieval of JSON behavior constants
  - : JSONB modification function with relaxed error handling
  - : Conversion from PostgreSQL Datum to JSON item for path execution

## Notes and Other Information
- This function is registered as the input function for the JSONB type in PostgreSQL's type system
- It uses the  parameter for , indicating standard (non-unique) processing
- The function context is passed through to support memory management and error handling
- Located in 