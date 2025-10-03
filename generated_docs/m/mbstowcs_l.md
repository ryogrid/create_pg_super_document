# mbstowcs_l

## Location
[src/backend/utils/adt/pg_locale.c:167-182](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/pg_locale.c#L167-L182)

## Overview
A locale-aware wrapper function that converts a multibyte character string to a wide character string using a specific locale.

## Definition

```c
static size_t
mbstowcs_l(wchar_t *dest, const char *src, size_t n, locale_t loc)
```
## Detailed Description
This function provides a portable implementation of locale-specific multibyte to wide character conversion. On Windows, it directly uses the system's  function. On other platforms, it temporarily switches to the specified locale using , performs the conversion with standard , then restores the original locale. This ensures thread-safe locale-specific character conversion across different operating systems.

## Parameters / Member Variables
- `*dest`: Pointer to the destination wide character array where converted characters will be stored
- `*src`: Pointer to the source multibyte character string to be converted
- `n`: Maximum number of wide characters to write to the destination array
- `loc`: The locale to use for the conversion
## Dependencies
- Functions called/Symbols referenced:
  -  (Windows only)
  -  (non-Windows platforms)
  -  (non-Windows platforms)
- Called from (representative examples):
  -  (src/backend/utils/adt/pg_locale.c:3184)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_locale.c file
- The function handles platform differences between Windows and POSIX systems for locale-specific character conversion
- On non-Windows platforms, the function saves and restores the current locale to ensure thread safety
- Returns the number of wide characters written to the destination array, or (size_t)-1 on error

## Simplified Source

```c
static size_t
mbstowcs_l(wchar_t *dest, const char *src, size_t n, locale_t loc)
{
#ifdef WIN32
    return _mbstowcs_l(dest, src, n, loc);
#else
    // Save current locale, switch to specified locale, convert, then restore
    size_t result;
    locale_t save_locale = uselocale(loc);

    result = mbstowcs(dest, src, n);
    uselocale(save_locale);
    return result;
#endif
}
```