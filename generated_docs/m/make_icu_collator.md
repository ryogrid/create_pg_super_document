# make_icu_collator

## Location
[src/backend/utils/adt/pg_locale.c:1472-1524](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L1472-L1524)

## Overview
make_icu_collator creates an ICU-based collator object for a PostgreSQL locale structure, with optional custom collation rules that extend the standard ICU collation behavior.

## Definition
```c
void make_icu_collator(const char *iculocstr, const char *icurules, struct pg_locale_struct *resultp)
```

## Detailed Description
This function initializes an ICU collator within a pg_locale_struct for use in PostgreSQL's internationalization system. It first creates a standard ICU collator using the specified locale string. If custom collation rules are provided, it retrieves the default rules from the standard collator, appends the custom rules, and creates a new collator with the combined rule set. The function handles memory management by storing the locale string in TopMemoryContext to ensure persistence across the backend's lifetime. The implementation is conditional on USE_ICU compilation flag and reports an appropriate error if ICU support is not available.

## Parameters / Member Variables
- `iculocstr`: ICU locale string specifying the base locale (e.g., "en-US", "de-DE")
- `icurules`: Optional custom collation rules to extend the standard collation behavior (NULL if not needed)
- `resultp`: Pointer to pg_locale_struct to be populated with the created ICU collator

## Dependencies
- Functions called/Symbols referenced:
  - [pg_ucol_open](../p/pg_ucol_open.md)
  - ucol_getRules
  - [icu_to_uchar](../i/icu_to_uchar.md)
  - palloc_array
  - ucol_close
  - ucol_openRules
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - ereport
- Called from (representative examples):
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
  - [CheckMyDatabase](../C/CheckMyDatabase.md)

## Notes and Other Information
- Only available when PostgreSQL is compiled with ICU support (USE_ICU)
- Memory leak potential exists if caller errors after collator creation but before proper cleanup
- Custom rules are appended to default ICU collation rules, allowing fine-tuned collation behavior
- The resulting collator is stored in TopMemoryContext for backend-lifetime persistence
- Handles Unicode character conversion for rule processing via icu_to_uchar
- Critical component for PostgreSQL's ICU-based internationalization and collation support