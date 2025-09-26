# parse_oid

## Location
src/bin/pg_combinebackup/pg_combinebackup.c: 789 - 822

## Overview
Parses a string representation of an Object Identifier (OID) with validation to ensure it represents a valid non-zero OID value.

## Definition


## Detailed Description
This function attempts to parse a string as a valid PostgreSQL Object Identifier (OID). It performs strict validation to ensure the input represents a valid OID:

1. Converts the string to an unsigned long integer using base-10 parsing
2. Validates that the conversion was successful (no parsing errors)
3. Ensures the entire string was consumed (no trailing characters)
4. Checks that the value is within the valid OID range (1 to PG_UINT32_MAX)
5. Rejects OIDs with value 0, as they are reserved and invalid

The function is designed to be robust against malformed input and provides clear success/failure indication through its boolean return value.

## Parameters / Member Variables
- : Input string to be parsed as an OID
- : Pointer to an Oid variable where the parsed result will be stored on success

## Dependencies
- Functions called/Symbols referenced:
  - : Standard C library function for string to unsigned long conversion
  - : Standard C library error indicator
  - : PostgreSQL constant defining maximum 32-bit unsigned integer value
- Called from (representative examples):
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:966)
  -  (in src/bin/pg_combinebackup/pg_combinebackup.c:1276)

## Notes and Other Information
- The function explicitly rejects OID value 0, which is reserved in PostgreSQL and considered invalid
- Uses strtoul() for parsing, which handles potential overflow conditions gracefully
- Validates that the entire input string represents a valid number (no partial parsing)
- Returns false for any malformed input, including strings with leading/trailing whitespace or non-numeric characters
- The function is used in pg_combinebackup utility for parsing OIDs from filesystem directory names, particularly for tablespace processing
- OIDs in PostgreSQL are 32-bit unsigned integers used as unique identifiers for database objects
- The validation ensures compatibility with PostgreSQL's internal OID handling and prevents invalid values from being processed