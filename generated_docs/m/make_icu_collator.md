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

## Simplified Source

```c
// Simplified version of make_icu_collator
void make_icu_collator(const char *iculocstr,
                      const char *icurules,
                      struct pg_locale_struct *resultp)
{
#ifdef USE_ICU
    UCollator *collator;

    // Step 1: Create basic ICU collator from locale string
    collator = pg_ucol_open(iculocstr);

    // Step 2: If custom rules provided, combine with default rules
    if (icurules) {
        const UChar *default_rules;
        UChar *combined_rules;
        UChar *custom_rules;
        int32_t length;

        // Get default rules from the standard collator
        default_rules = ucol_getRules(collator, &length);

        // Convert custom rules to Unicode format
        icu_to_uchar(&custom_rules, icurules, strlen(icurules));

        // Combine default + custom rules
        combined_rules = palloc_array(UChar, u_strlen(default_rules) + u_strlen(custom_rules) + 1);
        u_strcpy(combined_rules, default_rules);
        u_strcat(combined_rules, custom_rules);

        // Replace collator with one using combined rules
        ucol_close(collator);
        collator = ucol_openRules(combined_rules, u_strlen(combined_rules),
                                 UCOL_DEFAULT, UCOL_DEFAULT_STRENGTH, NULL, &status);

        if (U_FAILURE(status))
            ereport(ERROR, (errmsg("could not open collator for locale \"%s\" with rules \"%s\": %s",
                                   iculocstr, icurules, u_errorName(status))));
    }

    // Step 3: Store results in persistent memory context
    resultp->info.icu.locale = MemoryContextStrdup(TopMemoryContext, iculocstr);
    resultp->info.icu.ucol = collator;

#else
    // Error if ICU not supported
    ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                   errmsg("ICU is not supported in this build")));
#endif
}
```

Key simplifications made:
- Removed detailed variable declarations for cleaner flow
- Added step-by-step comments explaining the main algorithm phases
- Simplified error handling while preserving critical checks
- Consolidated Unicode string operations into logical blocks
- Maintained essential ICU function calls and memory management
- Preserved conditional compilation structure for ICU support