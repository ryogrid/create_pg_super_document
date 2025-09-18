# turkish_UTF_8_create_env

## Location
src/backend/snowball/libstemmer/stem_UTF_8_turkish.c: 2093 - 2094

## Overview
Factory function that creates and initializes a new Snowball environment structure specifically configured for Turkish UTF-8 text processing.

## Definition
extern struct SN_env * turkish_UTF_8_create_env(void)

## Detailed Description
This function serves as a language-specific wrapper around the generic SN_create_env function, providing the proper initialization parameters for Turkish stemming operations. It creates a Snowball environment with no integer variables (0) and one string variable (1), which matches the requirements of the Turkish stemming algorithm. The returned environment structure contains all necessary working memory, cursors, and state variables needed for Turkish text processing.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md)
- Called from (representative examples):
  - No direct references found (likely called through external interface or function pointers)

## Notes and Other Information
The function uses specific parameters (0, 1) for SN_create_env, indicating Turkish stemming requires no integer variables but uses one string variable for processing. This is part of the standard Snowball stemmer interface pattern where each language has its own create_env function with language-specific memory requirements. The returned environment must be properly cleaned up using the corresponding turkish_UTF_8_close_env function.