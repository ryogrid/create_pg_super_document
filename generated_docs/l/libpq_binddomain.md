# libpq_binddomain

## Location
[src/interfaces/libpq/fe-misc.c:1280-1328](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1280-L1328)

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
  - [pthread_mutex_lock](../p/pthread_mutex_lock.md)
  - [pthread_mutex_unlock](../p/pthread_mutex_unlock.md)
  - getenv (standard C library function)
  - bindtextdomain (gettext function)
  - PG_TEXTDOMAIN
  - PTHREAD_MUTEX_INITIALIZER
  - errno (on non-Windows platforms)
  - GetLastError/SetLastError (on Windows)
- Called from (representative examples):
  - [libpq_gettext](libpq_gettext.md)
  - [libpq_ngettext](libpq_ngettext.md)

## Notes and Other Information
- This is a static function, only accessible within fe-misc.c
- Uses double-checked locking pattern for thread-safe one-time initialization
- Preserves errno/GetLastError() to avoid side effects on error state
- The text domain is bound to "libpq" for message translation
- Supports both PGLOCALEDIR environment variable and compile-time LOCALEDIR fallback
- Critical for proper internationalization support in libpq client library

## Simplified Source

```c
static void
libpq_binddomain(void)
{
    // Thread-safe one-time initialization using double-checked locking
    static volatile bool already_bound = false;
    static pthread_mutex_t binddomain_mutex = PTHREAD_MUTEX_INITIALIZER;

    if (!already_bound) {
        // Save error state to avoid side effects
#ifdef WIN32
        int save_errno = GetLastError();
#else
        int save_errno = errno;
#endif

        // Lock mutex for thread safety
        pthread_mutex_lock(&binddomain_mutex);

        // Double-check pattern - test again inside lock
        if (!already_bound) {
            const char *ldir;

            // Determine locale directory
            ldir = getenv("PGLOCALEDIR");
            if (!ldir)
                ldir = LOCALEDIR;

            // Bind text domain for libpq translations
            bindtextdomain(PG_TEXTDOMAIN("libpq"), ldir);
            already_bound = true;
        }

        pthread_mutex_unlock(&binddomain_mutex);

        // Restore error state
#ifdef WIN32
        SetLastError(save_errno);
#else
        errno = save_errno;
#endif
    }
}
```