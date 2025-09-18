# PGTYPEStimestamp_current

## Location
src/interfaces/ecpg/pgtypeslib/timestamp.c: 294 - 303

## Overview
Retrieves the current system time and converts it to PostgreSQL's internal timestamp format for use in ECPG applications.

## Definition


## Detailed Description
PGTYPEStimestamp_current is an ECPG (Embedded SQL in C for PostgreSQL) library function that obtains the current system date and time and converts it to PostgreSQL's internal timestamp representation. This function provides a simple interface for ECPG applications to capture the current moment in time using PostgreSQL's timestamp data type.

The function operates by first calling GetCurrentDateTime to populate a tm structure with the current system time, then using tm2timestamp to convert this broken-down time representation into PostgreSQL's internal timestamp format. The conversion includes error checking through errno verification before performing the final timestamp conversion.

The function follows a void return pattern, writing the result directly to the provided timestamp pointer, which is consistent with other ECPG library functions that modify output parameters rather than returning values.

## Parameters / Member Variables
- `ts`: Pointer to a timestamp variable where the current time will be stored

## Dependencies
- Functions called/Symbols referenced:
  - GetCurrentDateTime (retrieves current system time into tm structure)
  - tm2timestamp (converts tm structure to PostgreSQL timestamp format)
  - errno (standard C error indicator)
- Called from (representative examples):
  - dtcurrent (Informix compatibility function)
  - Test programs (dt_test)

## Notes and Other Information
- Function modifies the timestamp through the output parameter rather than returning it
- Error handling relies on errno checking from GetCurrentDateTime before proceeding with conversion
- No explicit error return mechanism - callers should check errno if needed
- Part of the ECPG pgtypes library for embedded SQL applications
- Provides a simple interface for obtaining 'now' timestamps in embedded SQL contexts
- The microsecond parameter (second argument to tm2timestamp) is set to 0, indicating no sub-second precision from the system time call
- Relatively simple implementation focused on current time capture rather than complex time manipulation