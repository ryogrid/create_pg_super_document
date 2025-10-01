# inzcont

## Location
[src/timezone/zic.c:1556-1566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1556-L1566)

## Overview
Processes a timezone Zone continuation line from input by validating field count and delegating to inzsub for detailed processing.

## Definition

```c
struct zone z;
```
## Detailed Description
The  function handles timezone Zone continuation lines in the zic (zone information compiler) input. It performs simple validation to ensure the field count is within acceptable limits for continuation lines, then calls  with the continuation flag set to true. Zone continuation lines are used when a timezone definition spans multiple lines in the input file.

The function validates that the number of fields is between ZONEC_MINFIELDS (3) and ZONEC_MAXFIELDS (7), which is different from the regular Zone line field requirements.

## Parameters / Member Variables
- : Array of string pointers containing the parsed zone continuation fields from input
- : Number of fields provided in the fields array

## Dependencies
- Functions called/Symbols referenced:
  - [error](../e/error.md) (for error reporting)
  - [inzsub](inzsub.md) (to process zone continuation details with iscont=true)
- Called from (representative examples):
  - [infile](infile.md) (main input processing function)

## Notes and Other Information
- Returns false on validation errors, true on success
- Uses different field count limits than regular Zone lines (ZONEC_MINFIELDS/ZONEC_MAXFIELDS vs ZONE_MINFIELDS/ZONE_MAXFIELDS)
- Passes true as the iscont parameter to inzsub to indicate this is a continuation line
- Part of PostgreSQL's timezone data compilation system (zic)
- Continuation lines allow complex timezone definitions to span multiple input lines

## Simplified Source

```c
static bool
inzcont(char **fields, int nfields)
{
    // Validate field count for zone continuation lines
    if (nfields < ZONEC_MINFIELDS || nfields > ZONEC_MAXFIELDS) {
        error(_("wrong number of fields on Zone continuation line"));
        return false;
    }

    // Process the zone continuation data
    return inzsub(fields, nfields, true);
}
```