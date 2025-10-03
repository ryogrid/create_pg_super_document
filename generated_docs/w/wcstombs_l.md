# wcstombs_l

## Location
[src/backend/utils/adt/pg_locale.c:183-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L183-L212)

## Overview
A locale-aware wrapper function that converts a wide character string to a multibyte character string using a specific locale.

## Definition

```c
static size_t
wcstombs_l(char *dest, const wchar_t *src, size_t n, locale_t loc)
```
## Detailed Description
This function provides a portable implementation of locale-specific wide character to multibyte conversion. On Windows, it directly uses the system's  function. On other platforms, it temporarily switches to the specified locale using , performs the conversion with standard , then restores the original locale. This ensures thread-safe locale-specific character conversion across different operating systems.

## Parameters / Member Variables
- `*dest`: Pointer to the destination multibyte character array where converted characters will be stored
- `*src`: Pointer to the source wide character string to be converted
- `n`: Maximum number of bytes to write to the destination array
- `loc`: The locale to use for the conversion
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows only)
  -  (non-Windows platforms)
  -  (non-Windows platforms)
- Called from (representative examples):
  -  (src/backend/utils/adt/pg_locale.c:3122)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_locale.c file
- The function handles platform differences between Windows and POSIX systems for locale-specific character conversion
- On non-Windows platforms, the function saves and restores the current locale to ensure thread safety
- Returns the number of bytes written to the destination array (not including null terminator), or (size_t)-1 on error
- Complementary function to mbstowcs_l, performing the reverse conversion

## Simplified Source

```c
static size_t wcstombs_l(char *dest, const wchar_t *src, size_t n, locale_t loc) {
#ifdef WIN32
    // Windows: use system function directly
    return _wcstombs_l(dest, src, n, loc);
#else
    // POSIX: temporarily switch locale for conversion
    locale_t save_locale = uselocale(loc);
    size_t result = wcstombs(dest, src, n);
    uselocale(save_locale);  // restore original locale
    return result;
#endif
}
```