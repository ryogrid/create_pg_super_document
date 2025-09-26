# ExpandedRecordGetDatum

## Location
src/include/utils/expandedrecord.h: 143 - 148

## Overview
Converts an ExpandedRecordHeader into a read-write Datum for PostgreSQL's function manager system.

## Definition


## Detailed Description
This inline function provides a convenient wrapper to convert an ExpandedRecordHeader pointer into a Datum that can be used within PostgreSQL's function manager (fmgr) system. It delegates to the expanded object infrastructure by calling  on the header's embedded . The returned Datum represents a read-write reference to the expanded record, allowing the function manager to pass expanded objects efficiently between functions without unnecessary conversions to flat tuple format.

## Parameters / Member Variables
- : Pointer to an ExpandedRecordHeader structure containing the expanded record data and metadata

## Dependencies
- Functions called/Symbols referenced:
  - EOHPGetRWDatum
  - ExpandedRecordHeader
- Called from (representative examples):
  - PG_RETURN_EXPANDED_RECORD

## Notes and Other Information
- This is an inline function defined in the header file for performance
- Part of PostgreSQL's expanded object infrastructure for efficient handling of composite types
- The function provides read-write access, meaning the expanded record can be modified
- Used primarily in function manager contexts to return expanded records as Datums
- Located in src/include/utils/expandedrecord.h:143-148