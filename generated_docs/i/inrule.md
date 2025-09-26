# inrule

## Location
src/timezone/zic.c: 1471 - 1517

## Overview
Processes a timezone Rule line from input and adds the rule to the global rules array in the zic (zone information compiler) program.

## Definition

```c
struct rule r;
```
## Detailed Description
The  function parses and validates timezone rule data from input fields, then adds the validated rule to the global rules array. It performs field count validation, rule name validation, and extracts rule information including save time, DST flag, and abbreviation variables. The function is part of PostgreSQL's timezone compiler (zic) that processes timezone database files.

The function validates that the rule name doesn't start with whitespace, control characters, or digits (which are reserved). It then populates a static rule structure with the parsed data and calls  to handle the temporal aspects of the rule.

## Parameters / Member Variables
- : Array of string pointers containing the parsed rule fields from input
- : Number of fields provided in the fields array

## Dependencies
- Functions called/Symbols referenced:
  - error (for error reporting)
  - getsave (to parse save time and DST flag)
  - rulesub (to process temporal rule data)
  - ecpyalloc (to allocate and copy strings)
  - growalloc (to grow the rules array)
- Called from (representative examples):
  - infile (main input processing function)

## Notes and Other Information
- Uses field indices RF_NAME, RF_SAVE, RF_LOYEAR, RF_HIYEAR, RF_COMMAND, RF_MONTH, RF_DAY, RF_TOD, RF_ABBRVAR to access specific rule components
- Expects exactly RULE_FIELDS (10) fields in input
- Maintains max_abbrvar_len global variable to track maximum abbreviation length
- Rule names cannot start with whitespace, control characters, or digits
- Part of PostgreSQL's timezone data compilation system