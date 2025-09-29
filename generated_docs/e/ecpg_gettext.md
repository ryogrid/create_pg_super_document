# ecpg_gettext

## Location
[src/interfaces/ecpg/ecpglib/misc.c:482-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/misc.c#L482-L535)

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
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - bindtextdomain
  - dgettext
  - PG_TEXTDOMAIN (macro)
  - getenv
  - GetLastError/SetLastError (Windows)
- Called from (representative examples):
  - [ecpg_raise](ecpg_raise.md) (error reporting)
  - [ECPGconnect](../E/ECPGconnect.md) (connection handling)
  - [ECPGdescribe](../E/ECPGdescribe.md) (descriptor operations)
  - [ecpg_init](ecpg_init.md) (library initialization)

## Notes and Other Information
The function preserves errno/GetLastError() values around the bindtextdomain() call since that function may modify error codes. The implementation uses double-checked locking for performance optimization. The locale directory resolution prioritizes the PGLOCALEDIR environment variable over the compile-time LOCALEDIR constant. This function is essential for ECPG's internationalization support and is called extensively throughout the ECPG library for error messages and user-facing text.

## Simplified Source

```c
char *
ecpg_gettext(const char *msgid)
{
    static volatile bool already_bound = false;
    static pthread_mutex_t binddomain_mutex = PTHREAD_MUTEX_INITIALIZER;

    // One-time initialization of text domain
    if (!already_bound) {
        // Preserve error state during initialization
        int save_errno = errno;

        pthread_mutex_lock(&binddomain_mutex);

        if (!already_bound) {
            const char *ldir;

            // Get locale directory from environment or default
            ldir = getenv("PGLOCALEDIR");
            if (!ldir)
                ldir = LOCALEDIR;

            bindtextdomain(PG_TEXTDOMAIN("ecpglib"), ldir);
            already_bound = true;
        }

        pthread_mutex_unlock(&binddomain_mutex);
        errno = save_errno;
    }

    // Return localized message
    return dgettext(PG_TEXTDOMAIN("ecpglib"), msgid);
}
```