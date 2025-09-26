# inzsub

## Location
[src/timezone/zic.c:1567-1665](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1567-L1665)

## Overview
Processes the detailed parsing and validation of timezone zone data fields for both initial zone lines and zone continuation lines in the zic compiler.

## Definition

```c
struct zone z;
```
## Detailed Description
The  function is the core zone processing function that handles the detailed parsing of timezone zone data. It processes both regular Zone lines and Zone continuation lines based on the  parameter. The function:

- Sets up field indices based on whether this is a continuation line (ZFC_* constants) or regular zone line (ZF_* constants)
- Validates the zone name for regular zones using namecheck()
- Parses standard offset using gethms()
- Validates abbreviation format strings (must contain %s or %z if %)
- Processes optional UNTIL fields for zone transitions
- Validates continuation line chronological ordering
- Adds the processed zone to the global zones array

The function returns true if there are UNTIL fields (indicating more zone data follows), false otherwise.

## Parameters / Member Variables
- : Array of string pointers containing the parsed zone fields from input
- : Number of fields provided in the fields array  
- : Boolean indicating if this is a zone continuation line (true) or initial zone line (false)

## Dependencies
- Functions called/Symbols referenced:
  - namecheck (validates zone names)
  - ecpyalloc (allocates and copies strings)
  - gethms (parses time offset strings)
  - strchr (string character search)
  - rulesub (processes UNTIL rule data)
  - rpytime (calculates time from rule)
  - growalloc (grows the zones array)
  - error/warning (reporting functions)
- Called from (representative examples):
  - inzone (for initial zone lines)
  - inzcont (for zone continuation lines)

## Notes and Other Information
- Uses different field index constants for continuation lines (ZFC_*) vs regular zones (ZF_*)
- Validates abbreviation format strings - only %s and %z are allowed with %
- Handles format specifier 'z' by converting to 's' for compatibility
- Maintains max_format_len global variable for format string length tracking
- Performs chronological validation for continuation lines to ensure proper ordering
- Returns whether the zone has UNTIL fields indicating more data follows
- Part of PostgreSQL's timezone data compilation system (zic)