# anytime_typmodin

## Location
[src/backend/utils/adt/date.c:50-70](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/date.c#L50-L70)

## Overview
Common code for processing type modifiers for TIME and TIMETZ data types, validating the input array and extracting the precision modifier.

## Definition


## Detailed Description
This static function serves as a common implementation for both  and  functions. It processes type modifier input for TIME data types (with or without time zone), extracting and validating the precision specification. The function parses an array of type modifiers, ensures exactly one modifier is provided (the precision), and delegates the actual validation to .

## Parameters / Member Variables
- : Boolean flag indicating whether this is for a TIME WITH TIME ZONE type (true) or TIME WITHOUT TIME ZONE type (false)
- : ArrayType pointer containing the type modifier array from the SQL parser

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayGetIntegerTypmods](../A/ArrayGetIntegerTypmods.md) (extracts integer type modifiers from array)
  - [anytime_typmod_check](anytime_typmod_check.md) (validates the precision value)
- Called from (representative examples):
  - [timetypmodin](../t/timetypmodin.md) (src/backend/utils/adt/date.c:1562)
  - [timetztypmodin](../t/timetztypmodin.md) (src/backend/utils/adt/date.c:2387)

## Notes and Other Information
- This is a static helper function that consolidates common logic for TIME type modifier processing
- The function enforces that exactly one type modifier (precision) must be specified
- Error messages are intentionally brief since the SQL grammar should prevent invalid modifier counts
- Returns the validated type modifier value after checking via 
- Part of PostgreSQL's type system infrastructure for handling TIME data type variants