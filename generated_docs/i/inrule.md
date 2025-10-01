# inrule

## Location
[src/timezone/zic.c:1471-1517](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/zic.c#L1471-L1517)

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
  - [error](../e/error.md) (for error reporting)
  - [getsave](../g/getsave.md) (to parse save time and DST flag)
  - [rulesub](../r/rulesub.md) (to process temporal rule data)
  - [ecpyalloc](../e/ecpyalloc.md) (to allocate and copy strings)
  - [growalloc](../g/growalloc.md) (to grow the rules array)
- Called from (representative examples):
  - [infile](infile.md) (main input processing function)

## Notes and Other Information
- Uses field indices RF_NAME, RF_SAVE, RF_LOYEAR, RF_HIYEAR, RF_COMMAND, RF_MONTH, RF_DAY, RF_TOD, RF_ABBRVAR to access specific rule components
- Expects exactly RULE_FIELDS (10) fields in input
- Maintains max_abbrvar_len global variable to track maximum abbreviation length
- Rule names cannot start with whitespace, control characters, or digits
- Part of PostgreSQL's timezone data compilation system

## Simplified Source

```c
static void
inrule(char **fields, int nfields)
{
    static struct rule r;

    // Validate field count
    if (nfields != RULE_FIELDS) {
        error(_("wrong number of fields on Rule line"));
        return;
    }

    // Validate rule name - cannot start with whitespace, control chars, or digits
    char first_char = *fields[RF_NAME];
    if (first_char == '\0' || isspace(first_char) || first_char == '+' ||
        first_char == '-' || isdigit(first_char)) {
        error(_("Invalid rule name \"%s\""), fields[RF_NAME]);
        return;
    }

    // Set file tracking info
    r.r_filename = filename;
    r.r_linenum = linenum;

    // Parse save time and DST flag
    r.r_save = getsave(fields[RF_SAVE], &r.r_isdst);

    // Process temporal rule data (years, month, day, time)
    rulesub(&r, fields[RF_LOYEAR], fields[RF_HIYEAR], fields[RF_COMMAND],
            fields[RF_MONTH], fields[RF_DAY], fields[RF_TOD]);

    // Copy rule name and abbreviation
    r.r_name = ecpyalloc(fields[RF_NAME]);
    r.r_abbrvar = ecpyalloc(fields[RF_ABBRVAR]);

    // Track maximum abbreviation length
    if (max_abbrvar_len < strlen(r.r_abbrvar))
        max_abbrvar_len = strlen(r.r_abbrvar);

    // Add rule to global array
    rules = growalloc(rules, sizeof *rules, nrules, &nrules_alloc);
    rules[nrules++] = r;
}
```