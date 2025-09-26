# DateTimeErrorExtra

## Location
[src/include/utils/datetime.h:290-296](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/datetime.h#L290-L296)

## Overview
DateTimeErrorExtra is a structure that provides additional context information for datetime parsing errors, specifically for timezone-related error conditions in PostgreSQL's datetime processing system.

## Definition

Source: src/include/utils/datetime.h:290-296

## Detailed Description
DateTimeErrorExtra serves as a supplementary error information structure that enhances datetime parsing error reporting by providing specific details about timezone-related failures. This structure is used in conjunction with datetime error codes to give users more informative error messages when timezone parsing fails. The structure specifically supports two types of timezone errors: invalid timezone names (DTERR_BAD_TIMEZONE) and invalid timezone abbreviations (DTERR_BAD_ZONE_ABBREV). By storing the problematic timezone name and abbreviation, PostgreSQL can generate more helpful error messages that show users exactly what input caused the parsing failure.

## Parameters / Member Variables
- : Pointer to the incorrect timezone name string that caused a DTERR_BAD_TIMEZONE or DTERR_BAD_ZONE_ABBREV error
- : Pointer to the relevant timezone abbreviation string that caused a DTERR_BAD_ZONE_ABBREV error

## Dependencies
- Functions called/Symbols referenced:
  - (No direct symbol references - uses basic C string pointers)

- Called from (representative examples):
  - [DecodeDateTime](DecodeDateTime.md) (src/backend/utils/adt/datetime.c:980)
  - [DecodeTimeOnly](DecodeTimeOnly.md) (src/backend/utils/adt/datetime.c:1866)
  - [DecodeTimezoneAbbrev](DecodeTimezoneAbbrev.md) (src/backend/utils/adt/datetime.c:3093)
  - [DecodeTimezoneName](DecodeTimezoneName.md) (src/backend/utils/adt/datetime.c:3195)
  - [DecodeTimezoneAbbrevPrefix](DecodeTimezoneAbbrevPrefix.md) (src/backend/utils/adt/datetime.c:3309)
  - [DateTimeParseError](DateTimeParseError.md) (src/backend/utils/adt/datetime.c:4092)
  - [FetchDynamicTimeZone](../F/FetchDynamicTimeZone.md) (src/backend/utils/adt/datetime.c:4971)
  - [timestamp_in](../t/timestamp_in.md) (src/backend/utils/adt/timestamp.c:183)
  - [timestamptz_in](../t/timestamptz_in.md) (src/backend/utils/adt/timestamp.c:435)
  - [date_in](../d/date_in.md) (src/backend/utils/adt/date.c:128)
  - [time_in](../t/time_in.md) (src/backend/utils/adt/date.c:1393)
  - [timetz_in](../t/timetz_in.md) (src/backend/utils/adt/date.c:2292)

## Notes and Other Information
- This structure is specifically designed to work with DTERR_BAD_TIMEZONE and DTERR_BAD_ZONE_ABBREV error conditions
- The structure uses const char pointers, indicating that it references existing string data rather than copying it
- Both fields may be used together when a timezone abbreviation error occurs, providing both the problematic abbreviation and the associated timezone name
- This is part of PostgreSQL's comprehensive error reporting system that aims to provide users with actionable error information
- The structure enables the datetime parsing system to generate detailed error messages that help users identify and correct timezone-related input problems