# anytime_typmodout

## Location
src/backend/utils/adt/date.c: 93 - 112

## Overview
Common code for generating string representations of type modifiers for TIME and TIMETZ data types, formatting precision information with appropriate time zone notation.

## Definition


## Detailed Description
This static function serves as shared implementation for both  and  functions. It converts internal type modifier representations back into human-readable string format that matches SQL syntax. The function formats the precision value (if non-negative) along with the appropriate time zone specification, producing strings like "(3) with time zone" or "without time zone".

## Parameters / Member Variables
- : Boolean flag indicating whether this is for a TIME WITH TIME ZONE type (true) or TIME WITHOUT TIME ZONE type (false)
- : The internal type modifier value representing precision (-1 for default, >= 0 for specific precision)

## Dependencies
- Functions called/Symbols referenced:
  - psprintf (formats string with precision when typmod >= 0)
  - pstrdup (duplicates time zone string when typmod < 0)
- Called from (representative examples):
  - timetypmodout (src/backend/utils/adt/date.c:1570)
  - timetztypmodout (src/backend/utils/adt/date.c:2395)

## Notes and Other Information
- This is a static helper function that consolidates common logic for TIME type modifier output formatting
- When typmod is negative (indicating default precision), only the time zone specification is included
- When typmod is non-negative, both precision and time zone specification are included in parentheses format
- The returned string is allocated in the current memory context and should be managed by the caller
- Part of PostgreSQL's type system infrastructure for displaying type information in system catalogs and error messages
- The output format matches standard SQL syntax for TIME type declarations