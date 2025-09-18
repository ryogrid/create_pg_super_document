# regclassrecv

## Location
src/backend/utils/adt/regproc.c: 1000 - 1009

## Overview
Converts external binary format data to regclass type by delegating to the standard OID binary input function.

## Definition


## Detailed Description
The  function is the binary input function for the  data type. It handles the conversion of binary format data (typically from network protocols or binary storage) into the internal regclass representation.

Since regclass is fundamentally an OID (Object Identifier) internally, this function simply delegates to  to perform the actual binary-to-OID conversion. This design reflects the fact that regclass and OID have identical binary representations - the difference lies only in their text input/output formatting and semantic meaning.

This function is part of PostgreSQL's type input/output function framework, specifically handling binary format input for regclass values in contexts such as:
- Network protocol communication (binary format)
- Binary data storage formats
- Data exchange with client libraries using binary protocols

## Parameters / Member Variables
- Input:  (FunctionCallInfo) - Function call information containing binary data to convert

## Dependencies
- Functions called/Symbols referenced:
  -  - Binary input function for OID type

- Called from (representative examples):
  - (No direct references found - typically called by PostgreSQL's type system infrastructure)

## Notes and Other Information
- Shares implementation with oidrecv due to identical binary representation
- Part of the regclass type I/O function set alongside regclassin, regclassout, and regclasssend  
- Used in binary protocol contexts rather than text-based input/output
- The binary format for regclass is identical to OID: 4-byte big-endian integer
- Automatically invoked by PostgreSQL's type system when binary input is required
- No validation of the OID value is performed at this level - validation happens at higher levels