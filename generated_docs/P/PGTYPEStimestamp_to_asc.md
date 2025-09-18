# PGTYPEStimestamp_to_asc

## Location
src/interfaces/ecpg/pgtypeslib/timestamp.c: 272 - 293

## Overview
Converts a PostgreSQL internal timestamp value to its ASCII string representation for use in ECPG applications.

## Definition


## Detailed Description
PGTYPEStimestamp_to_asc is an ECPG (Embedded SQL in C for PostgreSQL) library function that converts PostgreSQL's internal timestamp data type to a human-readable ASCII string representation. This function serves as the inverse operation to PGTYPEStimestamp_from_asc.

The function handles both finite and special timestamp values. For special timestamps (positive and negative infinity), it uses EncodeSpecialTimestamp to generate appropriate string representations. For finite timestamps, it first converts the internal timestamp to a broken-down time structure using timestamp2tm, then uses EncodeDateTime to format it according to ISO date style.

The function uses a fixed DateStyle of 1 (USE_ISO_DATES) and allocates a new string using pgtypes_strdup to return the result. Error handling is implemented through errno setting when timestamp conversion fails.

## Parameters / Member Variables
- `tstamp`: The internal timestamp value to be converted to ASCII string format

## Dependencies
- Functions called/Symbols referenced:
  - TIMESTAMP_NOT_FINITE (macro to check for special timestamp values)
  - EncodeSpecialTimestamp (encodes infinity values to strings)
  - timestamp2tm (converts timestamp to broken-down time structure)
  - EncodeDateTime (formats datetime as string)
  - pgtypes_strdup (allocates and duplicates string)
  - MAXDATELEN (constant defining maximum date string length)
- Called from (representative examples):
  - dttoasc (Informix compatibility function)
  - ecpg_store_input (ECPG input storage function)
  - Extensive usage in test programs (dt_test, dt_test2, array tests)

## Notes and Other Information
- Returns a dynamically allocated string that must be freed by the caller
- Uses ISO date format (DateStyle = 1) consistently, with a comment suggesting this could potentially be made configurable
- Buffer size is limited to MAXDATELEN + 1 characters for the internal formatting
- Sets errno to PGTYPES_TS_BAD_TIMESTAMP and returns NULL on conversion failure
- Part of the ECPG pgtypes library for embedded SQL applications
- Extensively tested as evidenced by numerous test program references
- The function handles all timestamp representations including special values like infinity