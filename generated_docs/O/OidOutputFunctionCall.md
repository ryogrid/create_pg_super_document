# OidOutputFunctionCall

## Location
src/backend/utils/fmgr/fmgr.c: 1763 - 1771

## Overview
OidOutputFunctionCall is a convenience function that calls a datatype output function identified by its OID to convert internal Datum values to their string representation.

## Definition


## Detailed Description
OidOutputFunctionCall provides a simple interface for calling datatype output functions when you only have the function's OID rather than a pre-cached FmgrInfo structure. The function internally sets up the function manager info using fmgr_info() and then calls OutputFunctionCall() to perform the actual conversion from internal Datum format to string representation.

Like its input counterpart, this function is intended for seldom-executed code paths due to performance overhead from function lookup on each call and potential memory leakage issues. For performance-critical or frequently executed code, it's recommended to pre-cache the FmgrInfo structure and use OutputFunctionCall directly.

The function is widely used throughout PostgreSQL for debugging, logging, error reporting, and data export scenarios where convenience is more important than optimal performance.

## Parameters / Member Variables
- : OID of the output function to call for the datatype conversion
- : Internal Datum value to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - fmgr_info
  - OutputFunctionCall
- Called from (representative examples):
  - brin_minmax_multi_summary_out
  - debugtup
  - BuildIndexValueDescription
  - ExecBuildSlotValueDescription
  - SPI_getvalue
  - BildParamLogString
  - logicalrep_write_tuple
  - datum_to_json_internal
  - ri_ReportViolation
  - get_const_expr

## Notes and Other Information
- Like OidInputFunctionCall, this function is slow and may leak memory, so use sparingly
- Commonly used in debugging contexts, error reporting, and data serialization scenarios
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1763-1771
- Extensively used in PL/Perl, PL/Tcl, and JSON conversion functions
- For performance-critical code, cache the FmgrInfo and use OutputFunctionCall directly