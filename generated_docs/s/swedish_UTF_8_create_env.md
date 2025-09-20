# swedish_UTF_8_create_env

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_swedish.c:289-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_swedish.c#L289-L290)

## Overview
Creates a new Snowball stemming environment specifically configured for Swedish UTF-8 text processing.

## Definition

```c
}

extern struct SN_env * swedish_UTF_8_create_env(void)
```
## Detailed Description
This function is a wrapper around the generic SN_create_env function that creates a Snowball stemming environment tailored for Swedish language text processing in UTF-8 encoding. It initializes the environment with specific parameters suitable for Swedish stemming algorithms, including the appropriate string and integer variable counts needed by the Swedish stemming rules.

The function calls SN_create_env with parameters (0, 2), where:
- 0 indicates no string variables are needed
- 2 indicates that 2 integer variables are required for the Swedish stemming algorithm

## Parameters / Member Variables


## Return Value
- Returns a pointer to a newly allocated SN_env structure configured for Swedish UTF-8 stemming, or NULL if allocation fails

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of the Snowball stemming library integrated into PostgreSQL
- Located in src/backend/snowball/libstemmer/stem_UTF_8_swedish.c:289
- Must be paired with swedish_UTF_8_close_env to properly free allocated resources
- The environment created by this function should be used with Swedish stemming functions
- This is an external interface function that can be called from outside the module