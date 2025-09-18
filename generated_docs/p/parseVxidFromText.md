# parseVxidFromText

## Location
src/backend/utils/time/snapmgr.c: 1337 - 1366

## Overview
parseVxidFromText is a static parsing utility function that extracts PostgreSQL virtual transaction ID (VXID) values from formatted text lines during snapshot import operations.

## Definition
```c
static void parseVxidFromText(const char *prefix, char **s, const char *filename, VirtualTransactionId *vxid)
```

## Detailed Description
parseVxidFromText is an internal parsing subroutine used by ImportSnapshot to parse virtual transaction IDs from exported snapshot files. Unlike the other parsing functions, this function parses a compound value consisting of two components: a process number and a local transaction ID, formatted as "procNumber/localTransactionId".

The function performs the same validation pattern as other parsing functions:
1. **Prefix Verification**: Ensures the line starts with the expected prefix (e.g., "vxid:")
2. **VXID Parsing**: Uses sscanf with "%d/%u" format to extract both the signed process number and unsigned local transaction ID
3. **Line Termination**: Verifies the line ends with a newline character
4. **Position Advancement**: Updates the parsing pointer to the next line

The parsed values are stored directly into the provided VirtualTransactionId structure, making this function unique among the parsing utilities as it populates a structure rather than returning a simple value.

## Parameters / Member Variables
- `prefix`: Expected prefix string that should appear at the start of the line (typically "vxid:")
- `s`: Pointer to a character pointer that tracks the current parsing position; updated to point to the next line
- `filename`: Name of the snapshot file being parsed, used in error messages for diagnostics
- `vxid`: Pointer to a VirtualTransactionId structure that will be populated with the parsed values

## Dependencies
- Functions called/Symbols referenced:
  - strlen (standard C library)
  - strncmp (standard C library)
  - sscanf (standard C library)
  - strchr (standard C library)
  - ereport (PostgreSQL error reporting)
  - [VirtualTransactionId](../V/VirtualTransactionId.md) (PostgreSQL data structure)
- Called from (representative examples):
  - ImportSnapshot (for parsing the virtual transaction ID of the snapshot exporter)

## Notes and Other Information
- This is a static function, only accessible within the snapmgr.c compilation unit
- Used exclusively by ImportSnapshot to parse the virtual transaction ID field from snapshot files
- Returns void and populates the provided VirtualTransactionId structure via pointer parameter
- Uses "%d/%u" format specifier to parse the procNumber (signed int) and localTransactionId (unsigned int)
- The virtual transaction ID identifies the session and transaction that exported the snapshot
- Essential for tracking the origin of the snapshot and ensuring proper visibility semantics