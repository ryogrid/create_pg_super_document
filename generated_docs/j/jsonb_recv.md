# jsonb_recv

## Location
src/backend/utils/adt/jsonb.c: 89 - 107

## Overview
The  function is the binary receive function for the JSONB data type, responsible for converting binary-encoded JSON data received over PostgreSQL's network protocol into internal JSONB format.

## Definition


## Detailed Description
This function handles the reception of JSONB data transmitted in binary format over PostgreSQL's wire protocol. The binary format includes a version number prefix to allow for future format changes while maintaining backward compatibility. Currently, only version 1 is supported, which stores the JSON data as text following the version byte. The function extracts the version number, validates it, retrieves the JSON text, and delegates the actual parsing to .

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to:
  -  (StringInfo): Input buffer containing the binary-encoded JSONB data with version prefix

## Dependencies
- Functions called/Symbols referenced:
  - : Protocol function to extract integer values from message buffer
  - : Protocol function to extract text data from message buffer
  - : Core function that performs JSON parsing and JSONB conversion
  - : PostgreSQL logging/error reporting function
  - : Macro to extract pointer argument from function call
- Called from (representative examples):
  - No direct references found (typically called by PostgreSQL's type system during binary protocol operations)

## Notes and Other Information
- This function is registered as the binary receive function for the JSONB type in PostgreSQL's type system
- The binary format starts with a 1-byte version number (currently only version 1 is supported)
- Version 1 format stores JSON data as text, making the binary format essentially text-based
- Future versions could implement true binary JSON formats for improved performance
- The function uses NULL for the memory context parameter in 
- Located in 
- Throws an error for unsupported version numbers to ensure protocol compatibility