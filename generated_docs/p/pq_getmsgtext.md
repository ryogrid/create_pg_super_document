# pq_getmsgtext

## Location
src/backend/libpq/pqformat.c: 546 - 578

## Overview
Extracts a counted text string from a message buffer with optional character encoding conversion, always returning a freshly allocated null-terminated string.

## Definition


## Detailed Description
The  function retrieves a text string of specified length from a PostgreSQL message buffer and performs character encoding conversion from client encoding to server encoding if necessary. It always returns a pointer to a freshly 'd result that is null-terminated. The function also returns the actual byte length of the converted string through the  output parameter. If no conversion is needed, it creates a copy of the original data with a null terminator added.

## Parameters / Member Variables
- : A  structure representing the message buffer being read from
- : The number of raw bytes to extract from the message buffer (must be non-negative)
- : Output parameter that receives the actual byte length of the converted/copied string

## Dependencies
- Functions called/Symbols referenced:
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
  -  (character encoding conversion function)
  -  (PostgreSQL memory allocation)
  -  (memory copy function)
  -  (string length function)
- Called from (representative examples):
  -  (enum type receive function)
  -  (JSON type receive function)
  -  (JSONB type receive function)
  -  (JSON path type receive function)
  -  (name type receive function)
  -  (C string type receive function)
  -  (blank-padded char type receive function)
  -  (varchar type receive function)
  -  (text type receive function)
  -  (unknown type receive function)

## Notes and Other Information
- Always returns a freshly allocated string using , requiring the caller to manage memory
- Automatically adds null termination to ensure the result is a valid C string
- Performs character encoding conversion from client to server encoding when necessary
- Returns the actual converted string length through the  parameter, which may differ from  after conversion
- Validates data availability before processing to prevent buffer overruns
- Advances the message cursor automatically to maintain proper position tracking
- Commonly used for receiving text-based data types in PostgreSQL protocol messages
- The returned string length in  reflects the post-conversion size, not the original raw bytes