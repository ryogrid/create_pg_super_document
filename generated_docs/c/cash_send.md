# cash_send

## Location
src/backend/utils/adt/cash.c: 601 - 615

## Overview
Converts a PostgreSQL cash value to binary format for transmission over network connections or storage in binary format.

## Definition


## Detailed Description
The  function is part of PostgreSQL's binary I/O system for the money/cash data type. It takes a cash value as input and converts it to a binary representation using PostgreSQL's standard binary output protocol. This function is typically called when cash values need to be transmitted to clients in binary format or when performing binary serialization operations.

The function uses PostgreSQL's standard binary output functions to create a properly formatted binary representation that can be safely transmitted over network connections and later reconstructed using the corresponding receive function.

## Parameters / Member Variables
- Function follows PostgreSQL's standard function calling convention (PG_FUNCTION_ARGS)
- Input: A single cash value retrieved via 

## Dependencies
- Functions called/Symbols referenced:
  -  (data type)
  -  (macro to extract cash argument)
  -  (initialize binary output buffer)
  -  (send 64-bit integer in binary format)
  -  (finalize binary output buffer)
  -  (return binary data)
- Called from: 
  - Used internally by PostgreSQL's type system for binary output operations

## Notes and Other Information
- The cash data type is internally represented as a 64-bit integer
- This function is part of the binary I/O interface for the money data type
- The binary format ensures platform-independent representation of cash values
- Located in src/backend/utils/adt/cash.c:597-609