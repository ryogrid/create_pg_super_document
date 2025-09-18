# TmToChar

## Location
src/backend/utils/adt/formatting.c: 482 - 487

## Overview
A comprehensive structure that extends the fmt_tm structure to include fractional seconds and timezone information for datetime-to-character conversion operations.

## Definition


## Detailed Description
The TmToChar structure is a composite data type used by PostgreSQL's formatting system to represent complete datetime information for conversion to character strings. It builds upon the fmt_tm structure by adding fractional seconds precision and timezone name information. This structure serves as the primary container for datetime data during formatting operations, providing all the temporal components needed to generate formatted datetime strings. The structure is designed to handle both simple timestamps and complex timezone-aware datetime values with sub-second precision.

## Parameters / Member Variables
- : A fmt_tm structure containing the basic datetime components (year, month, day, hour, minute, second, etc.)
- : Fractional seconds component of type fsec_t, providing sub-second precision
- : A constant character pointer to the timezone name string for timezone-aware timestamps

## Dependencies
- Functions called/Symbols referenced:
  - fmt_tm (embedded structure)
  - fsec_t (fractional seconds type)
- Called from (representative examples):
  - DCH_ZONED
  - DCH_to_char
  - datetime_to_char_body
  - timestamp_to_char
  - timestamptz_to_char
  - interval_to_char

## Notes and Other Information
This structure is central to PostgreSQL's datetime formatting functionality in src/backend/utils/adt/formatting.c. It provides a complete representation of temporal data that includes not only the standard datetime components but also fractional seconds for high precision and timezone information for localization. The structure is particularly important for formatting operations that need to preserve sub-second precision and timezone context. The tzn field points to timezone name strings, allowing formatted output to include human-readable timezone information.