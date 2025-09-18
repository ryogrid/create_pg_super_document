# hungarian_ISO_8859_2_create_env

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c:859-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c#L859-L860)

## Overview
Creates a Snowball stemming environment specifically configured for Hungarian language text processing using the ISO-8859-2 character encoding.

## Definition
```c
extern struct SN_env * hungarian_ISO_8859_2_create_env(void)
```

## Detailed Description
This function is a language-specific wrapper around the generic SN_create_env function, designed for Hungarian text stemming operations. It initializes a Snowball environment with parameters tailored for Hungarian language processing using the ISO-8859-2 (Latin-2) character encoding, which is commonly used for Central and Eastern European languages including Hungarian.

The function calls SN_create_env(0, 1), indicating it creates an environment with:
- 0 string arrays (S_size = 0)  
- 1 integer array (I_size = 1)

This configuration is optimized for the Hungarian stemming algorithm's memory requirements.

## Parameters / Member Variables
- No parameters - this function takes void as input

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the Snowball stemming library integrated into PostgreSQL
- Specifically designed for ISO-8859-2 character encoding support
- Returns NULL on memory allocation failure (handled by SN_create_env)
- Should be paired with hungarian_ISO_8859_2_close_env to properly clean up resources
- Located in src/backend/snowball/libstemmer/stem_ISO_8859_2_hungarian.c:859