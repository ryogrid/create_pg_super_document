# r_is_reserved_word

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2005-2015](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L2005-L2015)

## Overview
Checks if the current word is a Turkish reserved word that should not be stemmed, specifically detecting the words "ad" and "soyad" (name and surname).

## Definition

```c
}

static int r_is_reserved_word(struct SN_env * z)
```
## Detailed Description
This function implements a reserved word check for the Turkish stemmer to prevent stemming of certain important words that should remain unchanged. The function specifically checks for:

1. Words ending with "ad" (Turkish word for "name")
2. Optionally preceded by "soy" to form "soyad" (Turkish word for "surname")
3. Ensures the match occurs at the beginning of the word (cursor at left boundary)

The function uses backward string matching to detect these patterns and returns 1 if a reserved word is found, preventing further stemming operations on these words.

## Parameters / Member Variables
- `*z`: Pointer to the Snowball environment structure containing the string being processed and cursor positions
## Dependencies
- Functions called/Symbols referenced:
  - [eq_s_b](../e/eq_s_b.md) (Snowball function for backward string equality testing)
- Called from:
  - [r_postlude](r_postlude.md) (at src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2043)

## Notes and Other Information
- This is a static function within the Turkish stemmer implementation
- Returns 1 if a reserved word is detected, 0 otherwise
- Critical for preserving important Turkish words like personal name-related terms
- Part of the final validation phase in Turkish word stemming
- Generated automatically by Snowball 2.2.0 stemmer generator
- Helps maintain semantic meaning by preventing over-stemming of culturally significant words

## Simplified Source

```c
static int r_is_reserved_word(struct SN_env * z) {
    // Check if word ends with "ad" (Turkish for "name")
    if (!eq_s_b(z, 2, s_16)) return 0;

    // Try to match "soyad" (surname) - optional "soy" prefix
    int saved_pos = z->l - z->c;
    if (!eq_s_b(z, 3, s_17)) {
        z->c = z->l - saved_pos;  // Restore position if no match
    }

    // Ensure we're at the start of the word
    if (z->c > z->lb) return 0;

    return 1;  // Reserved word found
}
```

**Key Logic**: Detects Turkish reserved words "ad" (name) and "soyad" (surname) by checking for the "ad" suffix and optionally the "soy" prefix, ensuring the match occurs at word boundaries to prevent over-stemming of culturally significant terms.