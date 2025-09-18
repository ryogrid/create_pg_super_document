# OidReceiveFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1772 - 1781

## Overview
OidReceiveFunctionCall is a convenience function that calls a datatype binary-input (receive) function identified by its OID to convert binary data to internal Datum format.

## Definition


## Detailed Description
OidReceiveFunctionCall provides a simple interface for calling datatype binary-input (receive) functions when you only have the function's OID rather than a pre-cached FmgrInfo structure. The function internally sets up the function manager info using fmgr_info() and then calls ReceiveFunctionCall() to perform the actual conversion from binary format to internal Datum representation.

This function is the binary counterpart to OidInputFunctionCall, handling binary data instead of string representations. Like other Oid-based function call wrappers, it's intended for seldom-executed code paths due to performance overhead and potential memory issues. It's commonly used in protocol handling, replication, and data exchange scenarios where binary data needs to be converted to PostgreSQL's internal format.

The function reads binary data from a StringInfo buffer and converts it according to the specified datatype's receive function, which is the inverse operation of the send function.

## Parameters / Member Variables
- : OID of the receive function to call for the datatype conversion
- : StringInfo buffer containing the binary data to be converted
- : Type-specific parameter passed to the receive function (often the element type OID for container types)
- : Type modifier value providing additional type-specific information

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info
  - ReceiveFunctionCall
- Called from (representative examples):
  - slot_store_data
  - slot_modify_data
  - parse_fcall_arguments
  - exec_bind_message

## Notes and Other Information
- Like other OidXXXFunctionCall functions, this is slow and may leak memory, so use sparingly
- Primarily used in binary protocol handling, logical replication, and fast-path function calls
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1772-1781
- For performance-critical code, cache the FmgrInfo and use ReceiveFunctionCall directly
- The StringInfo buffer parameter allows efficient binary data processing without additional copying