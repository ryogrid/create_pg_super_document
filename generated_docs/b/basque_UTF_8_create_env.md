# basque_UTF_8_create_env

## Location
src/backend/snowball/libstemmer/stem_UTF_8_basque.c: 1181 - 1182

## Overview
A factory function that creates and initializes a new Snowball environment specifically configured for Basque UTF-8 stemming operations.

## Definition
extern struct SN_env * basque_UTF_8_create_env(void)

## Detailed Description
The basque_UTF_8_create_env function serves as a specialized constructor for Basque stemming environments. It creates a new SN_env structure by calling the generic SN_create_env function with parameters specific to Basque language processing. The function passes 0 for the first parameter (likely indicating no special flags) and 3 for the second parameter (possibly indicating the number of string variables or buffer segments needed for Basque morphological processing). This environment will contain all the necessary state information, buffers, and cursors required for performing Basque stemming operations on UTF-8 encoded text.

## Parameters / Member Variables
- None (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [SN_create_env](../S/SN_create_env.md) (generic Snowball environment constructor)
- Called from (representative examples):
  - External stemming interface (no direct references found in indexed code)

## Notes and Other Information
This is a public interface function marked with extern for external linkage, typically called by higher-level stemming interfaces or PostgreSQL's text search infrastructure. The parameters (0, 3) are specific to the Basque stemming algorithm's requirements. The returned SN_env pointer must be properly managed and eventually freed using the corresponding basque_UTF_8_close_env function to prevent memory leaks.