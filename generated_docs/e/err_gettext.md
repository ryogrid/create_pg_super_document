# err_gettext

## Location
src/backend/utils/error/elog.c: 309 - 329

## Overview
Provides a safe wrapper around the gettext() function for message localization that automatically falls back to untranslated strings when error recursion is detected.

## Definition
```c
static inline const char *err_gettext(const char *str)
```

## Detailed Description
This function serves as a critical safety mechanism in PostgreSQL's internationalization (i18n) infrastructure. It wraps the standard gettext() function to provide localized error messages while preventing infinite recursion that could occur if the localization process itself triggers errors. When `in_error_recursion_trouble()` returns true, indicating that error handling has recursed too deeply, the function bypasses gettext() and returns the original untranslated string. This prevents potential infinite loops where translation errors cause more errors. When compiled without NLS (National Language Support), the function simply returns the original string unchanged. The function is aliased to the standard `_` macro throughout the elog.c module, making it the standard way to access localized strings in error handling code.

## Parameters / Member Variables
- `str`: The message string to be localized (or returned unchanged if recursion is detected or NLS is disabled)

## Dependencies
- Functions called/Symbols referenced:
  - [in_error_recursion_trouble](../i/in_error_recursion_trouble.md)
  - gettext (when ENABLE_NLS is defined)
- Called from (representative examples):
  - \_ macro (aliased throughout elog.c module)
  - Various error message formatting throughout PostgreSQL error handling

## Notes and Other Information
- Declared as `static inline` for performance optimization in the frequently-called error handling paths
- Includes `pg_attribute_format_arg(1)` attribute to help compilers check format string arguments
- When ENABLE_NLS is not defined, the function becomes a simple pass-through that returns the input string
- The function is aliased to the `_` macro via `#define _(x) err_gettext(x)` in elog.c
- This is a fallback mechanism specifically designed to prevent translation-related infinite recursion
- Critical for maintaining system stability when localization code encounters errors
- Part of PostgreSQL's robust error handling that gracefully degrades functionality under adverse conditions
- The recursion check ensures that even if gettext() itself causes errors, the system can still produce meaningful (though untranslated) error messages