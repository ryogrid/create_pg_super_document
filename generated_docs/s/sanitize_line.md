# sanitize_line

## Location
src/bin/pg_dump/dumputils.c: 50 - 101

## Overview
The sanitize_line function sanitizes strings to be safely included in SQL comments or TOC listings by replacing newlines with spaces, preventing dump corruption and potential SQL injection vulnerabilities.

## Definition


## Detailed Description
This function takes an input string and creates a sanitized copy suitable for inclusion in SQL comments or Table of Contents (TOC) listings. The primary purpose is to ensure that each logical output line remains as one physical output line by replacing any newline characters ('\n') or carriage return characters ('\r') with spaces.

This sanitization is critical for preventing corruption of PostgreSQL dumps, which could potentially create SQL injection vulnerabilities if someone were to load a dump containing database objects with maliciously crafted names containing newlines.

The function allocates a new string using pg_strdup and modifies it in place, ensuring the original input string remains unchanged.

## Parameters / Member Variables
- : The input string to be sanitized. If NULL, the function returns an empty string or hyphen based on want_hyphen parameter
- : Boolean flag that determines the return value when str is NULL. If true, returns "-", otherwise returns an empty string ""

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for memory allocation and string duplication)

- Called from (representative examples):
  - [PrintTOCSummary](../P/PrintTOCSummary.md) (in pg_backup_archiver.c)
  - [_printTocEntry](../p/_printTocEntry.md) (in pg_backup_archiver.c)
  - [dumpTableData](../d/dumpTableData.md) (in pg_dump.c)
  - [dumpUserConfig](../d/dumpUserConfig.md) (in pg_dumpall.c)
  - [dumpDatabases](../d/dumpDatabases.md) (in pg_dumpall.c)

## Notes and Other Information
- The function currently doesn't quote names, meaning the name fields in TOC listings aren't automatically parseable
- pg_restore -L doesn't require parsing these fields as it only examines the dumpId field
- Future enhancements might include better quoting mechanisms for improved parseability
- The function always returns a newly allocated string that must be freed by the caller
- Location: src/bin/pg_dump/dumputils.c:50-101