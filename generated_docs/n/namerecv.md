# namerecv

## Location
src/backend/utils/adt/name.c: 82 - 105

## Overview
The  function converts external binary format data to PostgreSQL's internal Name data type, used for receiving Name values through the binary protocol.

## Definition

```c
Datum
namerecv(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the binary input function for PostgreSQL's Name data type, designed to handle Name values transmitted through PostgreSQL's binary protocol (as opposed to text protocol). It receives binary-formatted data from a StringInfo buffer and converts it to the internal Name representation.

The function performs these key operations:
1. Extracts a StringInfo buffer containing the binary data using 
2. Reads the text data from the buffer using , which extracts the remaining bytes in the buffer
3. Validates that the identifier length doesn't exceed  characters
4. If the identifier is too long, throws an error with appropriate error codes and messages
5. Allocates zero-padded memory for the Name structure using 
6. Copies the data into the allocated Name structure
7. Frees the temporary string buffer
8. Returns the result as a Datum using 

This function provides strict length validation and proper error reporting when identifiers exceed PostgreSQL's naming limits.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: Pointer to StringInfo buffer containing binary data to be converted to Name type

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts pointer argument from function arguments
  - : Reads text from binary protocol message buffer
  - : PostgreSQL error reporting function
  - : Error code specification (ERRCODE_NAME_TOO_LONG)
  - : Error message specification
  - : Error detail message specification
  - : Zero-initialized memory allocation
  - : Memory copying function
  - : PostgreSQL memory deallocation
  - : Returns Name as Datum
  - : Maximum length constant for Name type
  - : PostgreSQL Name data type
  - : Name structure type
  - : PostgreSQL string buffer type

- Called from (representative examples):
  - This function is typically called by PostgreSQL's type system during binary protocol operations
  - No direct references found in the current analysis

## Notes and Other Information
- This is the binary protocol counterpart to  (which handles text protocol)
- Provides more strict error handling than  - throws an error instead of truncating oversize input
- Uses  to properly handle binary protocol message parsing
- The function expects the entire remaining buffer content to be the name string
- Memory management includes both allocation () and cleanup () of temporary buffers
- Part of PostgreSQL's binary I/O protocol system for efficient data transmission
- Ensures proper null-padding of Name structures like other Name I/O functions