# pg_saslprep

## Location
src/common/saslprep.c: 1049 - 1252

## Overview
The main function implementing SASLprep string normalization according to RFC 3454, used to normalize passwords and usernames for SASL authentication in PostgreSQL.

## Definition
```c
pg_saslprep_rc pg_saslprep(const char *input, char **output)
```

## Detailed Description
This function implements the complete SASLprep algorithm as specified in RFC 3454 for normalizing Unicode strings used in SASL authentication. SASLprep is essential for ensuring that passwords and usernames are processed consistently across different systems and locales.

The function follows the four-step SASLprep process:
1. **Mapping** - Maps certain characters to standard forms (e.g., non-ASCII spaces to ASCII space, some characters to nothing)
2. **Normalization** - Applies Unicode NFKC (Normalization Form KC) to handle character composition and compatibility
3. **Prohibition** - Rejects strings containing prohibited characters that could cause security issues
4. **Bidirectional checking** - Validates proper handling of right-to-left text according to Unicode bidirectional algorithm

The function includes an optimization for pure ASCII input (no processing needed) and comprehensive error handling for invalid UTF-8, prohibited characters, and memory allocation failures.

## Parameters / Member Variables
- `input`: Null-terminated UTF-8 encoded input string to normalize
- `output`: Pointer to receive the malloc'd/palloc'd normalized result string (set to NULL on failure)

## Dependencies
- Functions called/Symbols referenced:
  - pg_utf8_string_len (UTF-8 validation and length calculation)
  - pg_is_ascii (ASCII optimization check)
  - utf8_to_unicode (UTF-8 to Unicode codepoint conversion)
  - unicode_normalize (Unicode NFKC normalization)
  - unicode_to_utf8 (Unicode codepoint to UTF-8 conversion)
  - pg_utf_mblen (UTF-8 multibyte length calculation)
  - IS_CODE_IN_TABLE (codepoint range checking macro)
  - Memory allocation/deallocation macros (ALLOC, FREE, STRDUP)
- Called from (representative examples):
  - pg_be_scram_build_secret (backend SCRAM authentication)
  - scram_verify_plain_password (password verification)
  - pg_fe_scram_build_secret (frontend SCRAM authentication)
  - scram_init (SCRAM initialization)

## Notes and Other Information
- Returns pg_saslprep_rc enum values: SASLPREP_SUCCESS, SASLPREP_INVALID_UTF8, SASLPREP_PROHIBITED, or SASLPREP_OOM
- Critical component of PostgreSQL's SASL/SCRAM authentication system
- Handles memory management differently in frontend (malloc) vs backend (palloc)
- Implements comprehensive Unicode bidirectional text validation
- Empty passwords are explicitly prohibited after mapping
- Includes multiple Unicode character classification tables for prohibited, unassigned, and bidirectional character checking
- Essential for password security and authentication consistency across different Unicode representations