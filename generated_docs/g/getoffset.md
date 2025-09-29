# getoffset

## Location
[src/timezone/localtime.c:751-777](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/timezone/localtime.c#L751-L777)

## Overview
Extracts a timezone offset in [+-]hh[:mm[:ss]] format from a timezone string, handling both positive and negative offsets.

## Definition
```c
static const char *getoffset(const char *strp, int32 *const offsetp)
```

## Detailed Description
The getoffset function parses timezone offset specifications from timezone strings. It handles the optional leading sign (+ or -) followed by a time specification in hours, minutes, and seconds format. The function:

1. Checks for an optional leading sign character ('+' or '-')
2. Delegates the actual time parsing to the getsecs function
3. Applies the sign to the resulting offset value (negates if '-' was present)
4. Returns a pointer to the first character after the parsed offset

If no explicit sign is provided, the offset is treated as positive. The function supports the same flexible time formats as getsecs, including extended hour ranges for quasi-Posix timezone rules.

## Parameters / Member Variables
- `strp`: Pointer to the timezone string to parse, positioned at the start of the offset specification
- `offsetp`: Pointer to int32 where the calculated offset in seconds will be stored (positive or negative)

## Dependencies
- Functions called/Symbols referenced:
  - [getsecs](getsecs.md) (for parsing the time portion after the optional sign)
- Called from (representative examples):
  - [getrule](getrule.md)
  - [tzparse](../t/tzparse.md)

## Notes and Other Information
- This is a static function used internally within the timezone parsing subsystem
- The function treats unsigned offsets as positive by default
- Error handling is delegated to getsecs - if getsecs returns NULL, getoffset also returns NULL
- The offset is stored in seconds, making it easy to perform timezone calculations
- Both '+' and '-' signs are explicitly supported, with no sign defaulting to positive
- The function is used in both timezone rule parsing (getrule) and general timezone parsing (tzparse)

## Simplified Source

```c
// Simplified version of getoffset
static const char *
getoffset(const char *strp, int32 *const offsetp)
{
    bool neg = false;

    // Check for leading sign (+/-)
    if (*strp == '-') {
        neg = true;
        strp++;
    }
    else if (*strp == '+') {
        strp++;
    }

    // Parse the time portion (hh:mm:ss format)
    strp = getsecs(strp, offsetp);
    if (strp == NULL)
        return NULL;  // parsing failed

    // Apply negative sign if needed
    if (neg)
        *offsetp = -*offsetp;

    return strp;  // return pointer to next character
}
```

Key simplifications made:
- Preserved the complete original logic as it was already quite simple
- Added explanatory comments for each major step
- Clarified the purpose of each conditional block
- Maintained the exact algorithm flow since the function is concise and clear