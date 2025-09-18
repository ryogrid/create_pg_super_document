# pq_getmsgbytes

## Location
[src/backend/libpq/pqformat.c:508-527](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqformat.c#L508-L527)

## Overview
Extracts raw data bytes from a message buffer and returns a pointer directly into the buffer for efficient data access.

## Definition


## Detailed Description
The  function retrieves raw binary data from a PostgreSQL message buffer () by returning a pointer directly into the buffer's data area. This is an efficient zero-copy operation that allows direct access to the message data without creating a separate copy. The function advances the message cursor by the specified number of bytes and validates that sufficient data is available. The returned pointer may not have any particular memory alignment guarantees.

## Parameters / Member Variables
- : A  structure representing the message buffer being read from
- : The number of bytes to extract from the message buffer (must be non-negative)

## Dependencies
- Functions called/Symbols referenced:
  -  (for error reporting)
  -  (error level constant)
  -  (error code function)
  -  (error code constant)
  -  (error message function)
- Called from (representative examples):
  -  (SASL authentication)
  -  (function call argument parsing)
  -  (bind message execution)
  -  (array aggregation deserialization)
  -  (multirange type receive function)
  -  (range type receive function)
  -  (UUID type receive function)

## Notes and Other Information
- Returns a pointer directly into the message buffer, avoiding memory allocation overhead
- The returned pointer is not guaranteed to have any specific memory alignment
- Validates data availability before returning the pointer to prevent buffer overruns
- Advances the message cursor automatically to maintain proper position tracking
- Throws a protocol violation error if insufficient data is available
- Used extensively in type receive functions and protocol message parsing