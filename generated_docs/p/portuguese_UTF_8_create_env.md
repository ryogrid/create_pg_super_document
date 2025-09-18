# portuguese_UTF_8_create_env

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c:964-965](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_portuguese.c#L964-L965)

## Overview
The portuguese_UTF_8_create_env function creates and initializes a Snowball environment structure specifically configured for Portuguese UTF-8 text stemming operations.

## Definition
```c
extern struct SN_env * portuguese_UTF_8_create_env(void)
```

## Detailed Description
This function serves as a factory method for creating Portuguese stemming environments. It calls the generic SN_create_env function with parameters specifically tailored for Portuguese language processing. The function creates a Snowball environment with 0 string variables and 3 integer variables, which matches the requirements of the Portuguese stemming algorithm for tracking cursor positions and region boundaries.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct references found (likely called via external stemming interfaces)

## Notes and Other Information
The function is a thin wrapper around the generic environment creation function, providing Portuguese-specific configuration. The parameters (0, 3) indicate that the Portuguese stemmer requires no string variables but needs 3 integer variables for its operation. This is part of the language-specific API that allows the Snowball stemming library to be used with different languages while maintaining a consistent interface. The returned environment must be properly cleaned up using the corresponding close function when no longer needed.