# getNextFlagFromString

## Location
[src/backend/tsearch/spell.c:349-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L349-L454)

## Overview
A static function that parses and extracts individual affix flags from a string containing multiple flags, supporting different flag representation formats.

## Definition
```c
static void getNextFlagFromString(IspellDict *Conf, char **sflagset, char *sflag)
```

## Detailed Description
The `getNextFlagFromString` function is a crucial component of the PostgreSQL spell-checking system that handles the parsing of affix flags from compressed flag strings. It supports three different flag modes:

1. **FM_CHAR**: Single character flags (e.g., "ABCD" represents flags A, B, C, D)
2. **FM_LONG**: Two-character flags (e.g., "ABCDE*" represents flags AB, CD, E*)  
3. **FM_NUM**: Numeric flags separated by commas (e.g., "200,205,50" represents flags 200, 205, 50)

The function advances through the input string, extracting one flag at a time while validating the format according to the current flag mode. For numeric flags, it performs range checking (0 to FLAGNUM_MAXSIZE) and validates comma separation. It handles Unicode characters properly using PostgreSQL's multi-byte character functions.

## Parameters / Member Variables
- `Conf`: Pointer to IspellDict structure containing dictionary configuration including flagMode
- `sflagset`: Pointer to string pointer that gets advanced to the next flag position
- `sflag`: Output buffer where the extracted flag is stored as a null-terminated string

## Dependencies
- Functions called/Symbols referenced:
  - IspellDict (structure type)
  - FM_LONG, FM_CHAR, FM_NUM (flag mode constants)
  - COPYCHAR (macro for copying characters)
  - [pg_mblen](../p/pg_mblen.md) (PostgreSQL multi-byte length function)
  - FLAGNUM_MAXSIZE (maximum flag number constant)
  - [t_isdigit](../t/t_isdigit.md), t_iseq, t_isspace (text processing functions)
  - strtol (standard C library function)
  - sprintf (standard C library function)
  - ereport, elog (PostgreSQL error reporting functions)
- Called from (representative examples):
  - [IsAffixFlagInUse](../I/IsAffixFlagInUse.md)
  - [getCompoundAffixFlagValue](getCompoundAffixFlagValue.md)

## Notes and Other Information
- Modifies the sflagset pointer to advance to the next flag in the string
- Performs extensive error checking for malformed flag strings
- Handles Unicode characters properly through PostgreSQL's character handling functions
- The function does not return a value but updates the sflag output buffer
- Located in src/backend/tsearch/spell.c:349-454

## Simplified Source

```c
static void
getNextFlagFromString(IspellDict *Conf, char **sflagset, char *sflag)
{
    int32 s;
    char *next, *sbuf = *sflagset;
    int maxstep;
    bool stop = false;
    bool met_comma = false;

    maxstep = (Conf->flagMode == FM_LONG) ? 2 : 1;

    while (**sflagset) {
        switch (Conf->flagMode) {
            case FM_LONG:
            case FM_CHAR:
                // Copy character(s) to flag buffer
                COPYCHAR(sflag, *sflagset);
                sflag += pg_mblen(*sflagset);
                *sflagset += pg_mblen(*sflagset);

                // Check if we got all characters for this flag
                maxstep--;
                stop = (maxstep == 0);
                break;

            case FM_NUM:
                // Parse numeric flag
                s = strtol(*sflagset, &next, 10);

                // Validate numeric conversion and range
                if (*sflagset == next || s < 0 || s > FLAGNUM_MAXSIZE)
                    ereport(ERROR, /* flag format error */);

                sflag += sprintf(sflag, "%0d", s);
                *sflagset = next;

                // Skip to next number, handling comma separation
                while (**sflagset) {
                    if (t_isdigit(*sflagset)) {
                        if (!met_comma)
                            ereport(ERROR, /* missing comma */);
                        break;
                    } else if (t_iseq(*sflagset, ',')) {
                        if (met_comma)
                            ereport(ERROR, /* duplicate comma */);
                        met_comma = true;
                    } else if (!t_isspace(*sflagset)) {
                        ereport(ERROR, /* invalid character */);
                    }
                    *sflagset += pg_mblen(*sflagset);
                }
                stop = true;
                break;

            default:
                elog(ERROR, "unrecognized flagMode: %d", Conf->flagMode);
        }

        if (stop)
            break;
    }

    // Validate FM_LONG flag completeness
    if (Conf->flagMode == FM_LONG && maxstep > 0)
        ereport(ERROR, /* incomplete long flag */);

    *sflag = '\0';  // Null-terminate flag string
}
```