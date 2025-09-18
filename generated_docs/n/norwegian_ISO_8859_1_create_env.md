# norwegian_ISO_8859_1_create_env

## Location
src/backend/snowball/libstemmer/stem_ISO_8859_1_norwegian.c: 269 - 270

## Overview
Creates and initializes a Snowball environment for Norwegian language stemming using ISO 8859-1 character encoding.

## Definition
```c
extern struct SN_env * norwegian_ISO_8859_1_create_env(void)
```

## Detailed Description
This function serves as a language-specific wrapper for creating a Snowball stemming environment tailored for Norwegian text processing with ISO 8859-1 encoding. It calls the generic `SN_create_env()` function with parameters specific to the Norwegian stemming algorithm requirements. The function allocates and initializes the necessary data structures for performing Norwegian word stemming operations, including string arrays and integer arrays sized according to the algorithms needs.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - SN_create_env (called with parameters 0, 2 indicating 0 string arrays and 2 integer arrays)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the Snowball stemming library integrated into PostgreSQL for text search functionality
- The parameters (0, 2) passed to SN_create_env indicate that the Norwegian stemmer requires 0 string arrays and 2 integer arrays for its internal state
- Located in the auto-generated stemmer code for Norwegian language support
- Returns NULL if memory allocation fails during environment creation
- Must be paired with corresponding norwegian_ISO_8859_1_close_env() call to prevent memory leaks