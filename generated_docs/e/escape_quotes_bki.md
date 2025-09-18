# escape_quotes_bki

## Location
src/bin/initdb/initdb.c: 419 - 441

## Overview
Escapes and wraps field values in single quotes for safe insertion into BKI (Backend Interface) data files during database bootstrapping.

## Definition
```c
static char *escape_quotes_bki(const char *src)
```

## Detailed Description
This function prepares string values for insertion into BKI data files, which are used during PostgreSQL database initialization. It first applies standard quote escaping using escape_quotes(), then wraps the result in single quotes to create a properly formatted BKI field value. The function always adds single quotes around the value, even when not strictly necessary, to ensure consistent formatting. The escaping applied by this function will be reversed by the backend's DeescapeQuotedString() function when the BKI data is processed. This ensures that special characters in configuration values are properly preserved through the bootstrap process.

## Parameters / Member Variables
- `src`: The source string to be escaped and quoted for BKI format

## Dependencies
- Functions called/Symbols referenced:
  - escape_quotes (applies basic quote escaping)
  - pg_malloc (PostgreSQL memory allocation)
  - free (memory deallocation)
- Called from (representative examples):
  - AUTHTRUST_WARNING (in src/bin/initdb/initdb.c:295)
  - bootstrap_template1 (in src/bin/initdb/initdb.c:1569, 1575, 1578, 1581, 1584)

## Notes and Other Information
- Specifically designed for BKI data file format used during database bootstrapping
- Always wraps values in single quotes for consistent BKI formatting
- The escaping will be undone by DeescapeQuotedString() in the backend
- Returns a newly allocated string that must be freed by the caller
- Used primarily during template1 database bootstrap process
- Handles both the escaping and the quote wrapping in a single operation