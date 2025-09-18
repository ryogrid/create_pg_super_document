# from_char_seq_search

## Location
src/backend/utils/adt/formatting.c: 2719 - 2764

## Overview
A static function that provides a unified interface for performing case-insensitive sequential searches in either ASCII or localized string arrays, with integrated error handling and source string advancement for date/time parsing.

## Definition
```c
static bool from_char_seq_search(int *dest, const char **src, const char *const *array,
                                char **localized_array, Oid collid,
                                FormatNode *node, Node *escontext)
```

## Detailed Description
This function serves as a high-level wrapper around `seq_search_ascii()` and `seq_search_localized()`, automatically choosing the appropriate search method based on whether a localized array is provided. It handles the common pattern in date/time parsing where you need to find a matching string, advance the source pointer, and handle errors appropriately. The function determines which search algorithm to use: ASCII-only for English arrays or locale-aware for international arrays.

The function workflow:
1. Selects the appropriate search function based on availability of localized_array
2. Performs the search and retrieves the match length
3. If no match found, generates a detailed error message with context
4. If match found, advances the source pointer and returns success
5. Provides proper error handling with optional error contexts

## Parameters / Member Variables
- `dest`: Output parameter - receives the array index of the matched element
- `src`: Input/output parameter - pointer to source string, advanced past matched portion on success
- `array`: Array of English (ASCII) strings to search through
- `localized_array`: Optional array of localized strings; if NULL, uses ASCII search
- `collid`: Collation ID for locale-aware comparisons (used only with localized_array)
- `node`: FormatNode containing metadata for error reporting (specifically node->key->name)
- `escontext`: Error handling context - if present, returns false on error instead of throwing

## Dependencies
- Functions called/Symbols referenced:
  - seq_search_ascii (for ASCII string searches)
  - seq_search_localized (for localized string searches)
  - scanner_isspace (for whitespace detection in error messages)
  - ereturn (for error handling with context)
  - pstrdup (for string duplication in error reporting)
- Called from (representative examples):
  - DCH_ZONED (formatting.c:1067)
  - DCH_from_char (multiple locations in formatting.c for various date/time parsing)

## Notes and Other Information
- This is a static function, only accessible within formatting.c
- Automatically chooses between ASCII and localized search based on localized_array parameter
- Provides intelligent error messages that truncate at whitespace for readability
- Advances the source string pointer automatically on successful match
- Supports both traditional error throwing and soft error handling via escontext
- Primarily used in PostgreSQL's date/time parsing functionality
- Returns true on successful match, false on failure (when using error contexts)
- Error messages include the field name from the FormatNode for better user feedback