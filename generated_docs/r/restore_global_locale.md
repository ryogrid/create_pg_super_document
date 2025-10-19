# restore_global_locale

## Location
[src/bin/initdb/initdb.c:386-402](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L386-L402)

## Overview
Restores a previously saved global locale setting and frees the associated memory allocated by save_global_locale().

## Definition
```c
static void restore_global_locale(int category, save_locale_t save)
```

## Detailed Description
This function complements save_global_locale() by restoring a locale setting that was previously saved. It uses platform-specific locale restoration functions: _wsetlocale() on Windows for proper wide-character support, and setlocale() on other platforms. After successfully restoring the locale, it automatically frees the memory allocated for the saved locale name, completing the save-restore cycle. The function will terminate the program with a fatal error if locale restoration fails.

## Parameters / Member Variables
- `category`: The locale category to restore (must match the category used in save_global_locale())
- `save`: The saved locale value returned by save_global_locale()

## Dependencies
- Functions called/Symbols referenced:
  - save_locale_t (type definition)
  - setlocale (POSIX locale function)
  - _wsetlocale (Windows wide-character locale function)
  - free (memory deallocation)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error reporting)
- Called from (representative examples):
  - [locale_date_order](../l/locale_date_order.md) (in src/bin/initdb/initdb.c:2149)
  - [check_locale_name](../c/check_locale_name.md) (in src/bin/initdb/initdb.c:2210)

## Notes and Other Information
- Must be called with a value previously returned by save_global_locale()
- Automatically handles memory cleanup of the saved locale string
- Failure to restore results in program termination via pg_fatal()
- The category parameter should match the one used when saving the locale
- Part of initdb's locale management system for safe locale switching

## Simplified Source

```c
static void
restore_global_locale(int category, save_locale_t save)
{
#ifdef WIN32
    // Windows: Use wide-character setlocale
    if (!_wsetlocale(category, save))
        pg_fatal("failed to restore old locale");
#else
    // Unix: Use standard setlocale
    if (!setlocale(category, save))
        pg_fatal("failed to restore old locale \"%s\"", save);
#endif

    // Clean up allocated memory
    free(save);
}
```