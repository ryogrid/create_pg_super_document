# timestamptz_send

## Location
src/backend/utils/adt/timestamp.c: 847 - 857

## Overview
Converts PostgreSQL's internal timestamptz representation to binary format for efficient data transmission.

## Definition


## Detailed Description
The  function serves as the binary output function for the timestamptz data type. It takes a timestamptz value from PostgreSQL's internal representation and converts it to a binary format suitable for network transmission or binary storage. This function is part of PostgreSQL's binary I/O protocol, which provides more efficient data transfer compared to text-based formats by avoiding parsing overhead and reducing bandwidth usage.

The function is straightforward in its implementation: it extracts the timestamptz value, initializes a binary output buffer, writes the 64-bit timestamp value to the buffer, and returns the resulting bytea for transmission.

## Parameters / Member Variables
- Input: A timestamptz value obtained through 
- Output: A  containing the binary representation as bytea

## Dependencies
- Functions called/Symbols referenced:
  - : Macro to extract timestamptz argument
  - : Initializes a StringInfo buffer for binary output
  - : Writes a 64-bit integer to the binary buffer
  - : Finalizes the binary buffer and converts to bytea
  - : Macro to return the binary data as bytea
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL's type system)

## Notes and Other Information
- Located in src/backend/utils/adt/timestamp.c:847-857
- Part of PostgreSQL's binary I/O protocol for efficient data transfer
- Minimal implementation that simply writes the internal 64-bit timestamp value
- Used by PostgreSQL's type system infrastructure for binary protocol operations
- Counterpart to  for binary I/O operations
- The binary format preserves the exact internal representation without loss of precision
- Significantly more efficient than text-based output for bulk data operations