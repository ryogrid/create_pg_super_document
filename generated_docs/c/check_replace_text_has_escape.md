# check_replace_text_has_escape

## Location
[src/backend/utils/adt/varlena.c:4073-4105](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L4073-L4105)

## Overview
A static helper function that analyzes replacement text for backslash escape sequences to determine the optimal processing strategy for regular expression text replacement operations.

## Definition

```c
static int
check_replace_text_has_escape(const text *replace_text)
```
## Detailed Description
This function examines replacement text strings used in regular expression replacement operations to categorize the type of escape sequences present. It performs a single pass through the text looking for backslash characters and classifies them into three categories:

- **0**: No backslashes requiring processing - text can be used as-is
- **1**: Contains backslashes but no regexp submatch specifiers - requires basic escape processing  
- **2**: Contains regexp submatch specifiers (\1 through \9) - requires full submatch replacement

The function optimizes performance by returning immediately upon finding the highest-priority escape type (submatch specifiers), avoiding unnecessary scanning of the remaining text.

## Parameters / Member Variables
- `*replace_text`: Input text object to be analyzed for escape sequences and submatch specifiers
## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (macro for accessing text data)
  - VARSIZE_ANY_EXHDR (macro for getting text size excluding header)
  - memchr (C library function for finding characters)
- Called from (representative examples):
  - [replace_text_regexp](../r/replace_text_regexp.md)

## Notes and Other Information
- This is a static function internal to varlena.c, designed specifically for optimizing regexp replacement operations
- The function handles edge cases like backslashes at the end of strings gracefully
- The three-tier return value system allows calling functions to choose appropriate processing strategies based on complexity
- Located in src/backend/utils/adt/varlena.c:4073-4105

## Simplified Source

```c
static int check_replace_text_has_escape(const text *replace_text) {
    int result = 0;
    const char *current = VARDATA_ANY(replace_text);
    const char *end = current + VARSIZE_ANY_EXHDR(replace_text);

    // Scan for backslash escape sequences
    while (current < end) {
        // Find next backslash
        current = memchr(current, '\\', end - current);
        if (current == NULL) {
            break;  // No more backslashes
        }

        current++;  // Move past backslash
        if (current < end) {
            // Check what follows the backslash
            if (*current >= '1' && *current <= '9') {
                return 2;  // Found submatch specifier (\1-\9) - highest priority
            }
            result = 1;  // Found other escape sequence
            current++;
        }
    }

    return result;  // 0=no escapes, 1=basic escapes, 2=submatch specifiers
}
```