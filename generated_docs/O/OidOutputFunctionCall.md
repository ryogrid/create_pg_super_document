# OidOutputFunctionCall

## Location
[src/backend/utils/fmgr/fmgr.c:1763-1771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/fmgr/fmgr.c#L1763-L1771)

## Overview
OidOutputFunctionCall is a convenience function that calls a datatype output function identified by its OID to convert internal Datum values to their string representation.

## Definition

```c
char *
OidOutputFunctionCall(Oid functionId, Datum val)
```
## Detailed Description
OidOutputFunctionCall provides a simple interface for calling datatype output functions when you only have the function's OID rather than a pre-cached FmgrInfo structure. The function internally sets up the function manager info using fmgr_info() and then calls OutputFunctionCall() to perform the actual conversion from internal Datum format to string representation.

Like its input counterpart, this function is intended for seldom-executed code paths due to performance overhead from function lookup on each call and potential memory leakage issues. For performance-critical or frequently executed code, it's recommended to pre-cache the FmgrInfo structure and use OutputFunctionCall directly.

The function is widely used throughout PostgreSQL for debugging, logging, error reporting, and data export scenarios where convenience is more important than optimal performance.

## Parameters / Member Variables
- : OID of the output function to call for the datatype conversion
- : Internal Datum value to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - [OutputFunctionCall](OutputFunctionCall.md)
- Called from (representative examples):
  - [brin_minmax_multi_summary_out](../b/brin_minmax_multi_summary_out.md)
  - [debugtup](../d/debugtup.md)
  - [BuildIndexValueDescription](../B/BuildIndexValueDescription.md)
  - [ExecBuildSlotValueDescription](../E/ExecBuildSlotValueDescription.md)
  - [SPI_getvalue](../S/SPI_getvalue.md)
  - BildParamLogString
  - [logicalrep_write_tuple](../l/logicalrep_write_tuple.md)
  - [datum_to_json_internal](../d/datum_to_json_internal.md)
  - [ri_ReportViolation](../r/ri_ReportViolation.md)
  - get_const_expr

## Notes and Other Information
- Like OidInputFunctionCall, this function is slow and may leak memory, so use sparingly
- Commonly used in debugging contexts, error reporting, and data serialization scenarios
- The function is located in src/backend/utils/fmgr/fmgr.c at lines 1763-1771
- Extensively used in PL/Perl, PL/Tcl, and JSON conversion functions
- For performance-critical code, cache the FmgrInfo and use OutputFunctionCall directly