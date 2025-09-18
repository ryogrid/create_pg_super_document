# OidSendFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1782 - 1806

## Overview
OidSendFunctionCall is a convenience function that calls a datatype binary-output (send) function identified by its OID to convert internal Datum values to binary format.

## Definition


## Detailed Description
OidSendFunctionCall provides a simple interface for calling datatype binary-output (send) functions when you only have the function's OID rather than a pre-cached FmgrInfo structure. The function internally sets up the function manager info using fmgr_info() and then calls SendFunctionCall() to perform the actual conversion from internal Datum format to binary representation.

This function is the binary counterpart to OidOutputFunctionCall, producing binary data instead of string representations. Like other Oid-based function call wrappers, it's intended for seldom-executed code paths due to performance overhead and potential memory issues. It's commonly used in protocol handling, replication, and data exchange scenarios where data needs to be serialized in binary format for efficient transmission or storage.

The function ensures that the returned bytea result is not toasted, making it suitable for reliable data transmission and protocol operations.

## Parameters / Member Variables
- : OID of the send function to call for the datatype conversion
- : Internal Datum value to be converted to binary format

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info
  - SendFunctionCall
- Called from (representative examples):
  - logicalrep_write_tuple
  - SendFunctionResult

## Notes and Other Information
- Like other OidXXXFunctionCall functions, this is slow and may leak memory, so use sparingly
- Primarily used in logical replication and fast-path function call scenarios
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1782-1806
- For performance-critical code, cache the FmgrInfo and use SendFunctionCall directly
- Guarantees a non-toasted result, which is important for reliable data transmission
- The binary output is more compact and efficient than string representation for network protocols