# jsonb_send

## Location
src/backend/utils/adt/jsonb.c: 124 - 146

## Overview
The  function is the binary send function for the JSONB data type, responsible for converting internal JSONB values to binary format for transmission over PostgreSQL's network protocol.

## Definition


## Detailed Description
This function handles the transmission of JSONB data in binary format over PostgreSQL's wire protocol. It creates a versioned binary message that starts with a version number (currently 1) followed by the JSON text representation. The function first converts the JSONB value to its string representation, then packages it into a binary message with proper protocol formatting. This approach maintains compatibility while allowing for future binary format improvements.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (Jsonb*): The internal JSONB value to be converted to binary format for transmission

## Dependencies
- Functions called/Symbols referenced:
  - : Core function that serializes JSONB structures to JSON text
  - : Creates a new StringInfo buffer for text accumulation
  - : Frees a StringInfo buffer and its contents
  - : Initializes a message buffer for binary type transmission
  - : Sends an 8-bit integer to the message buffer
  - : Sends text data to the message buffer
  - : Finalizes the message buffer and returns the bytea result
  - : Macro to extract JSONB argument from function call
  - : Macro to get the size of a variable-length PostgreSQL data type
  - : Macro to return binary data from a PostgreSQL function
  - : Structure type representing internal JSONB data
- Called from (representative examples):
  - No direct references found (typically called by PostgreSQL's type system during binary protocol operations)

## Notes and Other Information
- This function is registered as the binary send function for the JSONB type in PostgreSQL's type system
- Uses version 1 format which stores JSON data as text after a version byte
- The binary format is designed to be extensible through version numbering
- Memory management includes proper cleanup of temporary StringInfo buffer
- Compatible with the  function which expects the same versioned format
- Future versions could implement more efficient binary representations
- Located in 
- Essential for client-server communication when using binary protocol mode