# AppendSeconds

## Location
src/interfaces/ecpg/pgtypeslib/interval.c: 733 - 758

## Overview
AppendSeconds is a static utility function that formats seconds and fractional seconds into a string buffer, providing precise control over fractional digit precision and zero-padding.

## Definition


## Detailed Description
This function appends seconds and optional fractional seconds to a character buffer at the specified position. It handles the formatting of time components with configurable precision for fractional seconds and optional zero-padding for the seconds component. The function is designed to build time strings incrementally without null-termination, allowing callers to continue appending additional components.

The function strips any sign from the input values and handles fractional seconds by building the fractional part in reverse order while skipping trailing zeros. If the specified precision is insufficient to represent the fractional seconds value, it falls back to using pg_ultostr() for a minimal correct representation.

## Parameters / Member Variables
- : Pointer to the current position in the output string buffer where seconds should be appended
- : Integer seconds value (sign will be stripped using abs())
- : Fractional seconds value of type fsec_t (essentially int32), representing microseconds
- : Maximum number of fractional digits to output (must be >= 0)
- : Boolean flag indicating whether to zero-pad the seconds to 2 digits

## Dependencies
- Functions called/Symbols referenced:
  - fsec_t (type definition for fractional seconds)
  - pg_ultostr_zeropad (for zero-padded integer-to-string conversion)
  - pg_ultostr (for standard integer-to-string conversion)
- Called from (representative examples):
  - AppendTimestampSeconds
  - EncodeTimeOnly
  - EncodeInterval (multiple locations in both backend and ecpg interface)

## Notes and Other Information
- The function does not null-terminate the output string; callers are responsible for proper string termination
- Sign handling is performed by stripping signs from both sec and fsec parameters using abs()
- Fractional seconds are processed to avoid trailing zeros in the output
- Located in src/backend/utils/adt/datetime.c:448-510
- Used extensively in time/interval formatting operations throughout PostgreSQL