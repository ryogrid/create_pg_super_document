# dutch_ISO_8859_1_create_env

## Location
[src/backend/snowball/libstemmer/stem_ISO_8859_1_dutch.c:600-601](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_ISO_8859_1_dutch.c#L600-L601)

## Overview
Creates a new Snowball stemmer environment instance specifically configured for Dutch language text processing using ISO 8859-1 character encoding.

## Definition
```c
extern struct SN_env * dutch_ISO_8859_1_create_env(void)
```

## Detailed Description
This function serves as a language-specific wrapper for creating a Snowball stemmer environment tailored for Dutch text processing with ISO 8859-1 encoding. It initializes the stemmer environment by calling the generic SN_create_env function with parameters specific to the Dutch language implementation. The function is part of PostgreSQL's full-text search infrastructure, specifically the Snowball stemming library integration that provides morphological analysis capabilities for Dutch text.

The function creates an environment with 3 string variables (second parameter) and 0 integer variables (first parameter), which are the specific requirements for the Dutch stemming algorithm implementation.

## Parameters / Member Variables
This function takes no parameters and returns a pointer to a newly allocated SN_env structure configured for Dutch ISO 8859-1 stemming.

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md) (called with parameters 0, 3)
- Called from (representative examples):
  - No direct references found in the current codebase

## Notes and Other Information
- This function is part of the auto-generated Snowball stemmer code for Dutch language support
- The returned environment must be properly cleaned up using dutch_ISO_8859_1_close_env to avoid memory leaks
- Located in the Snowball libstemmer integration within PostgreSQL's backend
- The ISO 8859-1 encoding variant specifically handles Western European character sets
- File location: src/backend/snowball/libstemmer/stem_ISO_8859_1_dutch.c:600