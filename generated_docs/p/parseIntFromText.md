# parseIntFromText

## Location
[src/backend/utils/time/snapmgr.c:1287-1311](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1287-L1311)

## Overview
parseIntFromText is a static parsing utility function that extracts integer values from formatted text lines during snapshot import operations.

## Definition
```c
static int parseIntFromText(const char *prefix, char **s, const char *filename)
```

## Detailed Description
parseIntFromText is an internal parsing subroutine used by ImportSnapshot to parse structured snapshot data from exported snapshot files. The function reads lines in the format "prefix:value" where the value is an integer, validates the format, extracts the integer value, and advances the parsing position to the next line.

The function performs strict validation:
1. **Prefix Verification**: Ensures the line starts with the expected prefix
2. **Integer Parsing**: Uses sscanf to extract the integer value after the colon
3. **Line Termination**: Verifies the line ends with a newline character
4. **Position Advancement**: Updates the parsing pointer to the next line

Any parsing errors result in detailed error messages that include the problematic filename for debugging purposes.

## Parameters / Member Variables
- `prefix`: Expected prefix string that should appear at the start of the line (e.g., "pid:", "dbid:")
- `s`: Pointer to a character pointer that tracks the current parsing position; updated to point to the next line
- `filename`: Name of the snapshot file being parsed, used in error messages for diagnostics

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library)
  - strncmp (standard C library)
  - sscanf (standard C library)
  - strchr (standard C library)
  - ereport (PostgreSQL error reporting)
- Called from (representative examples):
  - [ImportSnapshot](../I/ImportSnapshot.md) (multiple calls for parsing different integer fields)

## Notes and Other Information
- This is a static function, only accessible within the snapmgr.c compilation unit
- Used exclusively by ImportSnapshot to parse various integer fields from snapshot files
- Provides consistent error handling and validation for all integer fields in snapshot data
- The function modifies the input pointer to advance through the file content line by line
- Error messages use ERRCODE_INVALID_TEXT_REPRESENTATION for consistency with PostgreSQL error codes