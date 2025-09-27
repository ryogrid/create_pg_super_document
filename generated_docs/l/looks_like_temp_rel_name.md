# looks_like_temp_rel_name

## Location
[src/backend/storage/file/fd.c:3446-3494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L3446-L3494)

## Overview
Determines whether a given filename matches the naming pattern of a PostgreSQL temporary relation file.

## Definition
```c
bool looks_like_temp_rel_name(const char *name)
```

## Detailed Description
This function validates whether a filename follows the specific naming convention used for PostgreSQL temporary relation files. The expected pattern is:

`t<digits>_<digits>[_<forkname>][.<segment>]`

Where:
- Starts with literal "t"
- Followed by one or more digits (relation OID portion)
- Followed by an underscore "_"
- Followed by one or more digits (additional identifier)
- Optionally followed by "_<forkname>" (for different relation forks like main, fsm, vm)
- Optionally followed by ".<segment>" (for segmented relations, with digits indicating segment number)

The function parses each component sequentially, ensuring proper formatting at each step. It uses `forkname_chars()` to validate fork name components and checks that segment numbers are properly formatted as digits.

## Parameters / Member Variables
- `name`: The filename to check against temporary relation naming patterns

## Dependencies
- Functions called/Symbols referenced:
  - [forkname_chars](../f/forkname_chars.md)
  - isdigit (from standard C library)
- Called from (representative examples):
  - [RemovePgTempRelationFilesInDbspace](../R/RemovePgTempRelationFilesInDbspace.md)
  - [sendDir](../s/sendDir.md)

## Notes and Other Information
- Returns true if the name matches the temporary relation pattern, false otherwise
- The function is designed to be strict about the naming format to avoid accidentally identifying non-temporary files
- Fork names are validated using `forkname_chars()` which checks against PostgreSQLs standard fork name conventions
- Segment numbers must be purely numeric and non-empty
- Used in both cleanup operations (removing temp files) and backup operations (identifying temp files to potentially exclude)
- Part of PostgreSQLs file management system for distinguishing temporary relations from permanent ones

## Simplified Source

```c
// Simplified version of looks_like_temp_rel_name
bool looks_like_temp_rel_name(const char *name) {
    int pos;

    // Pattern: t<digits>_<digits>[_<forkname>][.<segment>]

    // Must start with "t"
    if (name[0] != 't') {
        return false;
    }

    // Parse first digit sequence (relation OID)
    pos = 1;
    if (!isdigit((unsigned char) name[pos])) {
        return false;  // Must have at least one digit
    }
    while (isdigit((unsigned char) name[pos])) {
        pos++;
    }

    // Must have underscore separator
    if (name[pos] != '_') {
        return false;
    }
    pos++;

    // Parse second digit sequence (additional identifier)
    int digit_start = pos;
    while (isdigit((unsigned char) name[pos])) {
        pos++;
    }
    if (pos == digit_start) {
        return false;  // Must have at least one digit
    }

    // Optional fork name: _<forkname>
    if (name[pos] == '_') {
        int forkchar = forkname_chars(&name[pos + 1], NULL);
        if (forkchar <= 0) {
            return false;  // Invalid fork name
        }
        pos += forkchar + 1;  // Skip '_' and fork name
    }

    // Optional segment: .<digits>
    if (name[pos] == '.') {
        pos++;  // Skip the dot
        int segment_start = pos;
        while (isdigit((unsigned char) name[pos])) {
            pos++;
        }
        if (pos == segment_start) {
            return false;  // Segment must have digits
        }
    }

    // Must be at end of string
    return (name[pos] == '\0');
}
```

Key simplifications made:
- Added inline comments explaining the expected pattern
- Clarified the parsing logic for each component
- Simplified the digit validation loops
- Made the optional components (fork name and segment) more explicit
- Removed unnecessary variables and consolidated logic
- Maintained all validation checks while improving readability