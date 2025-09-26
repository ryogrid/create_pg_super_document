# ecpg_gettext

## Location
src/interfaces/ecpg/ecpglib/misc.c: 482 - 535

## Overview
A thread-safe internationalization function for the ECPG library that initializes text domain binding and retrieves localized messages using gettext.

## Definition
```c
char *ecpg_gettext(const char *msgid)
```

## Detailed Description
This function provides thread-safe access to localized error messages and text strings in the ECPG library. It implements a one-time initialization pattern to safely call bindtextdomain() for the "ecpglib" text domain, which is necessary for proper internationalization support. The function uses mutex-based synchronization to ensure that bindtextdomain() is called exactly once per process, addressing platform-specific thread safety issues, particularly on Windows.

The function first checks if text domain binding has already been performed. If not, it acquires a mutex, sets up the locale directory (either from the PGLOCALEDIR environment variable or the default LOCALEDIR), and calls bindtextdomain() to establish the message catalog location. After initialization, it uses dgettext() to retrieve the localized version of the requested message.

## Parameters / Member Variables
- `msgid`: A message identifier string for which to retrieve the localized translation

## Dependencies
- Functions called/Symbols referenced:
  - pthread_mutex_lock
  - pthread_mutex_unlock
  - bindtextdomain
  - dgettext
  - PG_TEXTDOMAIN (macro)
  - getenv
  - GetLastError/SetLastError (Windows)
- Called from (representative examples):
  - ecpg_raise (error reporting)
  - ECPGconnect (connection handling)
  - ECPGdescribe (descriptor operations)
  - ecpg_init (library initialization)

## Notes and Other Information
The function preserves errno/GetLastError() values around the bindtextdomain() call since that function may modify error codes. The implementation uses double-checked locking for performance optimization. The locale directory resolution prioritizes the PGLOCALEDIR environment variable over the compile-time LOCALEDIR constant. This function is essential for ECPG's internationalization support and is called extensively throughout the ECPG library for error messages and user-facing text.