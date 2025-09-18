# yiddish_UTF_8_create_env

## Location
src/backend/snowball/libstemmer/stem_UTF_8_yiddish.c: 1230 - 1231

## Overview
Factory function that creates and initializes a new Snowball environment structure specifically configured for Yiddish UTF-8 stemming operations.

## Definition
```c
extern struct SN_env * yiddish_UTF_8_create_env(void)
```

## Detailed Description
This function serves as a language-specific wrapper around the generic SN_create_env function, providing the correct initialization parameters for Yiddish stemming. It creates a new Snowball environment with 0 string variables and 2 integer variables, which are the specific requirements for the Yiddish stemming algorithm. The returned environment structure will be used to maintain state during the stemming process.

The function abstracts away the specific parameter requirements for Yiddish stemming, providing a clean interface for clients who need to create a stemming environment for this language.

## Parameters / Member Variables
- None (void function)

## Dependencies  
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct callers found in the codebase (likely called through stemmer interface)

## Notes and Other Information
- Returns a pointer to newly allocated SN_env structure, or NULL on allocation failure
- The created environment must be freed using yiddish_UTF_8_close_env when no longer needed
- Parameters passed to SN_create_env: 0 string variables, 2 integer variables
- Part of the standard Snowball stemmer interface pattern where each language provides create/close functions
- Located in src/backend/snowball/libstemmer/stem_UTF_8_yiddish.c:1230