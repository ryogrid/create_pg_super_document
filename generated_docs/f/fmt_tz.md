# fmt_tz

## Location
src/backend/utils/adt/formatting.c: 434 - 444

## Overview
A structure used by PostgreSQL's formatting functions to store timezone information during timestamp parsing operations.

## Definition


## Detailed Description
The fmt_tz structure is a simple container used specifically by the do_to_timestamp function and related datetime parsing routines in PostgreSQL's formatting system. It serves as an output parameter to capture timezone-related information when parsing datetime strings that contain timezone specifications. The structure tracks whether any timezone fields were encountered during parsing and stores the calculated GMT offset in seconds.

## Parameters / Member Variables
- : Boolean flag indicating whether any timezone fields (TZ, TZH, TZM) were present in the input string being parsed
- : The calculated GMT offset in seconds, representing the timezone displacement from UTC

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a simple data structure)
- Called from (representative examples):
  - DCH_ZONED
  - to_timestamp
  - to_date  
  - parse_datetime
  - do_to_timestamp

## Notes and Other Information
This structure is used internally by PostgreSQL's datetime formatting and parsing system, specifically in src/backend/utils/adt/formatting.c. It's designed to be a lightweight container for timezone information extracted during the parsing of datetime strings with timezone components. The gmtoffset is stored in seconds to provide precise timezone offset calculations.