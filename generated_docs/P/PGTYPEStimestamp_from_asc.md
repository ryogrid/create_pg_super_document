# PGTYPEStimestamp_from_asc

## Location
src/interfaces/ecpg/pgtypeslib/timestamp.c: 206 - 271

## Overview
Parses a string representation of a timestamp and converts it to PostgreSQL's internal timestamp format for use in ECPG applications.

## Definition


## Detailed Description
PGTYPEStimestamp_from_asc is an ECPG (Embedded SQL in C for PostgreSQL) library function that converts a string representation of a timestamp into PostgreSQL's internal timestamp data type. This function handles various timestamp formats including regular dates, special values like epoch, infinity, and negative infinity. 

The function performs comprehensive parsing by first using ParseDateTime to break the input string into fields, then DecodeDateTime to interpret those fields into a structured time representation. Based on the decoded type, it handles different cases: regular dates are converted using tm2timestamp, while special values like epoch, infinity (late), and negative infinity (early) are handled with specific setter functions.

The function includes robust error handling, setting errno to PGTYPES_TS_BAD_TIMESTAMP for various error conditions including string length limits, parse failures, and invalid date types. It also provides optional end pointer functionality to indicate where parsing stopped in the input string.

## Parameters / Member Variables
- `str`: Input string containing the timestamp representation to be parsed
- `endptr`: Optional pointer to a char pointer that will be set to point to the first character after the parsed timestamp (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - ParseDateTime (parses date/time string into fields)
  - DecodeDateTime (interprets parsed fields into structured time)
  - tm2timestamp (converts tm structure to timestamp)
  - SetEpochTimestamp (creates epoch timestamp)
  - TIMESTAMP_NOEND (macro to set positive infinity)
  - TIMESTAMP_NOBEGIN (macro to set negative infinity)
  - strlen (standard C library function)
- Called from (representative examples):
  - dtcvasc (Informix compatibility function)
  - ecpg_get_data (ECPG data retrieval function)
  - Various test programs (dt_test, dt_test2, array tests)

## Notes and Other Information
- Maximum input string length is limited by MAXDATELEN constant
- Function sets errno to 0 on successful completion to distinguish from error cases
- Supports special timestamp values: epoch, positive infinity (late), negative infinity (early)
- Part of the ECPG pgtypes library for embedded SQL applications
- The commented-out AdjustTimestampForTypmod line suggests potential future support for timestamp precision adjustment
- Error handling uses PGTYPES_TS_BAD_TIMESTAMP errno value consistently across different failure modes