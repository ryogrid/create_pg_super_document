# map_locale

## Location
[src/port/win32setlocale.c:111-171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32setlocale.c#L111-L171)

## Overview
A static helper function that maps problematic locale names on Windows to their correct equivalents using a locale mapping table.

## Definition

```c
static const char *
map_locale(const struct locale_map *map, const char *locale)
```
## Detailed Description
The  function performs string substitution on locale names to fix Windows-specific locale naming issues. It searches through a provided mapping table for locale name patterns that need to be replaced. The function supports both simple string replacement and more complex pattern matching where it can replace text between two delimiters (like a simplified regex "start.*end" replacement).

The function uses a static buffer to store the modified locale name, avoiding dynamic memory allocation. If a match is found in the mapping table, it constructs a new locale string by copying the prefix, inserting the replacement text, and appending the suffix. If no matches are found, it returns the original locale string unchanged.

## Parameters / Member Variables
- : Pointer to an array of  structures that define the mapping rules
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: The input locale string that may need to be transformed

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (standard C library function)
  -  (standard C library function)
  -  (structure type)
  -  (constant defining buffer size)
- Called from:
  -  (twice, for different mapping scenarios)

## Notes and Other Information
- Returns a pointer to a static buffer  when a replacement is made, or the original LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL= pointer when no replacement is needed
- The static buffer has a maximum size of  (100 characters)
- Returns NULL if the resulting locale name would exceed the buffer size
- Supports two types of replacements: single string replacement and start/end delimiter replacement
- The function is Windows-specific and part of the port layer for locale handling

## Simplified Source

```c
static const char *map_locale(const struct locale_map *map, const char *locale) {
    static char aliasbuf[MAX_LOCALE_NAME_LEN];
    int i;

    // Search through mapping table for problematic locale patterns
    for (i = 0; map[i].locale_name_start != NULL; i++) {
        const char *needle_start = map[i].locale_name_start;
        const char *needle_end = map[i].locale_name_end;
        const char *replacement = map[i].replacement;
        char *match_start = NULL;
        char *match_end = NULL;

        // Find start pattern in locale string
        char *match = strstr(locale, needle_start);
        if (match) {
            match_start = match;
            if (needle_end) {
                // Two-part pattern: find end pattern after start
                match = strstr(match_start + strlen(needle_start), needle_end);
                if (match)
                    match_end = match + strlen(needle_end);
                else
                    match_start = NULL;  // No complete match
            } else {
                // Single pattern replacement
                match_end = match_start + strlen(needle_start);
            }
        }

        if (match_start) {
            // Build replacement string: prefix + replacement + suffix
            int matchpos = match_start - locale;
            int replacementlen = strlen(replacement);
            int restlen = strlen(match_end);

            // Check buffer size limit
            if (matchpos + replacementlen + restlen + 1 > MAX_LOCALE_NAME_LEN)
                return NULL;

            memcpy(&aliasbuf[0], &locale[0], matchpos);
            memcpy(&aliasbuf[matchpos], replacement, replacementlen);
            memcpy(&aliasbuf[matchpos + replacementlen], match_end, restlen + 1);

            return aliasbuf;
        }
    }

    // No transformation needed, return original
    return locale;
}
```