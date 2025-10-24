# normalize_libc_locale_name

## Location
[src/backend/commands/collationcmds.c:600-630](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L600-L630)

## Overview
normalize_libc_locale_name strips encoding tags from libc locale names to create normalized locale identifiers, enabling consistent locale name handling.

## Definition

```c
static bool
normalize_libc_locale_name(char *new, const char *old)
```
## Detailed Description
This static utility function processes libc locale names by removing encoding specifications (e.g., ".utf8", ".UTF-8", ".iso885915") while preserving other locale components. The normalization process:
1. Copies characters from the old name to the new name
2. When encountering a dot ('.'), skips over the encoding tag that follows
3. Encoding tags are identified as sequences of alphanumeric characters and hyphens
4. Preserves other locale components like country codes and modifiers (e.g., "@euro")

Examples of transformations:
- "en_US.utf8" → "en_US"
- "br_FR.iso885915@euro" → "br_FR@euro"

## Parameters / Member Variables
- `*new`: Output buffer to store the normalized locale name
- `*old`: Input locale name to be normalized
## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic C string operations)
- Called from (representative examples):
  - [pg_import_system_collations](../p/pg_import_system_collations.md)

## Notes and Other Information
- Static function used internally within collationcmds.c
- Returns true if the name was modified (encoding tag was found and removed)
- Used during system collation import to ensure consistent locale naming
- Handles various encoding tag formats commonly found in libc locale names
- Critical for creating canonical locale names that can be reliably matched and compared

## Simplified Source

```c
static bool normalize_libc_locale_name(char *new, const char *old) {
    char *n = new;
    const char *o = old;
    bool changed = false;

    while (*o) {
        if (*o == '.') {
            // Skip encoding tag (e.g., ".utf8", ".UTF-8")
            o++;
            while ((*o >= 'A' && *o <= 'Z') || (*o >= 'a' && *o <= 'z') ||
                   (*o >= '0' && *o <= '9') || (*o == '-'))
                o++;
            changed = true;
        } else {
            // Copy regular character
            *n++ = *o++;
        }
    }
    *n = '\0';

    return changed;  // True if encoding tag was removed
}
```