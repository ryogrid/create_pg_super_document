# turkish_UTF_8_close_env

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:2095-2096](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L2095-L2096)

## Overview
Cleanup function that properly deallocates and closes a Turkish UTF-8 Snowball environment structure, releasing all associated memory resources.

## Definition
extern void turkish_UTF_8_close_env(struct SN_env * z)

## Detailed Description
This function serves as the counterpart to turkish_UTF_8_create_env, providing proper cleanup and resource deallocation for Turkish stemming environments. It wraps the generic SN_close_env function with Turkish-specific parameters, ensuring that all memory allocated during environment creation is properly freed. The function uses parameter 0 for SN_close_env, which corresponds to the number of integer variables that need cleanup (matching the 0 used in turkish_UTF_8_create_env).

## Parameters / Member Variables
- : Pointer to the SN_env structure to be closed and deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [SN_close_env](../S/SN_close_env.md)
- Called from (representative examples):
  - No direct references found (likely called through external interface when stemming operations complete)

## Notes and Other Information
This function must be called for every environment created with turkish_UTF_8_create_env to prevent memory leaks. The parameter 0 passed to SN_close_env matches the integer variable count used in environment creation, ensuring consistent memory management. Following the Snowball stemmer pattern, this provides language-specific cleanup while leveraging the common underlying infrastructure.