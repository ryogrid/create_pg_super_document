# pg_ucol_open

## Location
src/backend/utils/adt/pg_locale.c: 2602 - 2683

## Overview
A wrapper around ICU's ucol_open() function that handles API differences and compatibility issues across different ICU versions.

## Definition
```c
static UCollator *pg_ucol_open(const char *loc_str)
```

## Detailed Description
The pg_ucol_open function is a critical wrapper that abstracts the complexity of opening ICU collators across different ICU library versions. It addresses several version-specific compatibility issues and provides consistent behavior regardless of the underlying ICU version.

Key features and workarounds:
1. **Default Collator Prevention**: Explicitly prevents opening the default collator (by passing NULL) to ensure consistent behavior independent of environment settings
2. **Root Locale Compatibility**: For ICU versions < 55, converts "und" (undefined language) to "root" since older versions don't recognize "und" as the root locale
3. **Legacy Attribute Setting**: For ICU versions < 54, manually sets collation attributes using icu_set_collation_attributes() after opening the collator
4. **Comprehensive Error Handling**: Provides detailed error messages and proper cleanup on failures

The function ensures that collators are opened consistently across different ICU versions while maintaining proper error reporting and resource management.

## Parameters / Member Variables
- `loc_str`: Locale string specifying the desired collation locale (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - ucol_open (ICU library function)
  - ucol_close (ICU library function)  
  - uloc_getLanguage (ICU library function)
  - icu_set_collation_attributes
  - u_errorName (ICU library function)
  - elog, ereport (PostgreSQL error reporting)
  - errmsg
  - palloc, pfree (PostgreSQL memory management)
  - strcmp, strcpy, strcat, strlen (standard C library functions)
- Called from (representative examples):
  - make_icu_collator
  - get_collation_actual_version
  - icu_validate_locale

## Notes and Other Information
- Static function (internal to pg_locale.c)
- Handles ICU version compatibility issues from versions < 54 through current
- Never allows opening the default collator to ensure environment independence
- Implements "und" to "root" locale conversion for older ICU versions
- Performs manual attribute setting for very old ICU versions (< 54)
- Returns a UCollator pointer that must be closed with ucol_close()
- Essential for PostgreSQL's ICU collation support across different ICU library versions
- Part of the ICU collation infrastructure that ensures consistent behavior