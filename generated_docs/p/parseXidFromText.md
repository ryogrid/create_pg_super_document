# parseXidFromText

## Location
[src/backend/utils/time/snapmgr.c:1312-1336](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/time/snapmgr.c#L1312-L1336)

## Overview
parseXidFromText is a static parsing utility function that extracts PostgreSQL transaction ID (XID) values from formatted text lines during snapshot import operations.

## Definition
```c
static TransactionId parseXidFromText(const char *prefix, char **s, const char *filename)
```

## Detailed Description
parseXidFromText is an internal parsing subroutine used by ImportSnapshot to parse PostgreSQL transaction IDs from exported snapshot files. The function reads lines in the format "prefix:value" where the value is an unsigned integer representing a TransactionId, validates the format, extracts the XID value, and advances the parsing position to the next line.

Similar to parseIntFromText, this function performs strict validation:
1. **Prefix Verification**: Ensures the line starts with the expected prefix (e.g., "xmin:", "xmax:", "xip:")
2. **XID Parsing**: Uses sscanf with "%u" format to extract the unsigned integer transaction ID
3. **Line Termination**: Verifies the line ends with a newline character  
4. **Position Advancement**: Updates the parsing pointer to the next line

The function is specifically designed for parsing transaction IDs, which are unsigned 32-bit integers in PostgreSQL, and provides appropriate error handling for malformed snapshot data.

## Parameters / Member Variables
- `prefix`: Expected prefix string that should appear at the start of the line (e.g., "xmin:", "xmax:", "xip:")
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
  - [ImportSnapshot](../I/ImportSnapshot.md) (multiple calls for parsing xmin, xmax, and transaction ID arrays)

## Notes and Other Information
- This is a static function, only accessible within the snapmgr.c compilation unit
- Used exclusively by ImportSnapshot to parse TransactionId fields from snapshot files
- Returns a TransactionId (unsigned 32-bit integer) rather than a signed integer like parseIntFromText
- Provides consistent error handling for all transaction ID fields in snapshot data
- Uses "%u" format specifier in sscanf to properly parse unsigned transaction IDs
- Essential for reconstructing the xip array and other transaction-related fields in imported snapshots