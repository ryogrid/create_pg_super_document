# SN_set_current

## Location
[src/backend/snowball/libstemmer/api.c:51-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/api.c#L51-L56)

## Overview
Sets the current input string in a Snowball stemming environment, preparing it for processing by stemming algorithms.

## Definition

```c
}

extern int SN_set_current(struct SN_env * z, int size, const symbol * s)
```
## Detailed Description
This function initializes the working buffer of a Snowball stemming environment with input text that needs to be processed. It replaces the entire content of the environment's primary string buffer with the provided input string and resets the current position cursor to the beginning. This is typically the first operation performed before running any stemming algorithm on a word.

The function uses  to perform the actual string replacement, replacing the entire current content (from position 0 to current length) with the new input string. After setting the content, it resets the cursor position () to 0, positioning it at the start of the string for processing.

## Parameters / Member Variables
- `*z`: Pointer to the SN_env stemming environment structure
- `size`: Length of the input string in symbols/characters
- `*s`: Pointer to the input string data to be processed
## Dependencies
- Functions called/Symbols referenced:
  - [replace_s](../r/replace_s.md) (performs the actual string replacement operation)
  - symbol (type reference for string data)

- Called from (representative examples):
  - [dsnowball_lexize](../d/dsnowball_lexize.md) (main entry point for Snowball stemming in PostgreSQL's dictionary system)

## Notes and Other Information
- Returns the result of the replace_s operation (typically 0 on success, non-zero on error)
- Always resets the cursor position to the beginning of the string
- This is the standard way to load input text into the Snowball stemming environment
- Used by PostgreSQL's text search dictionary system to prepare words for stemming
- Must be called before running language-specific stemming algorithms on the text

## Simplified Source

```c
extern int SN_set_current(struct SN_env *z, int size, const symbol *s) {
    // Replace entire string buffer with new input
    int err = replace_s(z, 0, z->l, size, s, NULL);

    // Reset cursor to beginning
    z->c = 0;

    return err;
}
```