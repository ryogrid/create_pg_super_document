# pg_database_encoding_character_incrementer

## Location
src/backend/utils/mb/mbutils.c: 1523 - 1545

## Overview
A dispatcher function that returns the appropriate character incrementer function based on the current database's encoding.

## Definition
```c
mbcharacter_incrementer pg_database_encoding_character_incrementer(void)
```

## Detailed Description
This function serves as a central dispatcher that provides encoding-specific character increment functionality. It examines the current database encoding and returns a function pointer to the appropriate character incrementer. This design allows PostgreSQL to handle different character encodings efficiently without runtime encoding checks in performance-critical paths.

The function currently supports:
- UTF-8 encoding: Returns pg_utf8_increment for proper UTF-8 byte sequence handling
- EUC-JP encoding: Returns pg_eucjp_increment for Japanese character set handling  
- All other encodings: Returns pg_generic_charinc as a fallback

This approach provides optimal performance for supported encodings while maintaining compatibility for all other encodings through the generic incrementer.

## Parameters / Member Variables
None - this function takes no parameters

## Dependencies
- Functions called/Symbols referenced:
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (retrieves current database encoding)
  - PG_UTF8 (UTF-8 encoding constant)
  - [pg_utf8_increment](pg_utf8_increment.md) (UTF-8 specific incrementer)
  - PG_EUC_JP (EUC-JP encoding constant)
  - [pg_eucjp_increment](pg_eucjp_increment.md) (EUC-JP specific incrementer)
  - [pg_generic_charinc](pg_generic_charinc.md) (generic fallback incrementer)
- Called from (representative examples):
  - [make_greater_string](../m/make_greater_string.md)

## Notes and Other Information
- Returns a function pointer of type mbcharacter_incrementer
- The comment suggests future enhancement might add this to pg_wchar_table[] for better organization
- Critical for PostgreSQL's LIKE pattern optimization and range type operations
- Enables encoding-aware character increment operations for text processing performance
- The generic fallback ensures compatibility with all PostgreSQL supported encodings