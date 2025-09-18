# libpq_binddomain

## Location
src/interfaces/libpq/fe-misc.c: 1280 - 1328

## Overview
Initializes the text domain binding for libpq internationalization support in a thread-safe manner.

## Definition
static void libpq_binddomain(void)

## Detailed Description
libpq_binddomain sets up the text domain binding for libpq's internationalization (i18n) system using gettext. The function ensures that bindtextdomain() is called exactly once per process in a thread-safe manner, as some gettext implementations (particularly on Windows) can fail when called concurrently from multiple threads.

The function uses a mutex and a static flag to implement the once-only initialization pattern. It determines the locale directory from the PGLOCALEDIR environment variable or falls back to the compile-time LOCALEDIR constant. The function also preserves errno/GetLastError() values to avoid interfering with error handling in calling code.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - pthread_mutex_lock
  - pthread_mutex_unlock
  - getenv (standard C library function)
  - bindtextdomain (gettext function)
  - PG_TEXTDOMAIN
  - PTHREAD_MUTEX_INITIALIZER
  - errno (on non-Windows platforms)
  - GetLastError/SetLastError (on Windows)
- Called from (representative examples):
  - libpq_gettext
  - libpq_ngettext

## Notes and Other Information
- This is a static function, only accessible within fe-misc.c
- Uses double-checked locking pattern for thread-safe one-time initialization
- Preserves errno/GetLastError() to avoid side effects on error state
- The text domain is bound to "libpq" for message translation
- Supports both PGLOCALEDIR environment variable and compile-time LOCALEDIR fallback
- Critical for proper internationalization support in libpq client library