# norwegian_UTF_8_create_env

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_norwegian.c:273-274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_norwegian.c#L273-L274)

## Overview
Creates a new Snowball stemming environment specifically configured for Norwegian language text processing with UTF-8 encoding.

## Definition

```c
}

extern struct SN_env * norwegian_UTF_8_create_env(void)
```
## Detailed Description
This function is part of the Snowball stemming library integration in PostgreSQL, specifically designed for Norwegian language stemming operations. It serves as a wrapper around the generic SN_create_env function, providing pre-configured parameters optimized for Norwegian text processing with UTF-8 character encoding. The function allocates and initializes a stemming environment that can be used to perform morphological analysis and word stemming on Norwegian text.

The function calls SN_create_env(0, 2), where the first parameter (0) represents the string size and the second parameter (2) represents the number of integer variables required for the Norwegian stemming algorithm.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md) (from src/backend/snowball/libstemmer/api.c:3)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the Snowball stemming library, which is used for full-text search functionality in PostgreSQL
- Located in stem_UTF_8_norwegian.c:273, indicating it's generated code from Snowball algorithms
- Returns a pointer to SN_env structure that must be properly closed using norwegian_UTF_8_close_env
- The returned environment is specifically tuned for Norwegian language morphology and UTF-8 text encoding
- This is likely auto-generated code from the Snowball compiler for the Norwegian stemming algorithm