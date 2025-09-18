# free_auth_token

## Location
src/backend/libpq/hba.c: 278 - 287

## Overview
A destructor function that properly cleans up an AuthToken structure, specifically handling the deallocation of any compiled regular expressions it may contain.

## Definition
```c
static void free_auth_token(AuthToken *token)
```

## Detailed Description
The free_auth_token function is responsible for properly cleaning up an AuthToken structure, with particular attention to releasing any compiled regular expression resources. The function first checks if the token contains a regular expression using token_has_regexp(), and if so, calls pg_regfree() to properly deallocate the regex structure.

This function is crucial for preventing memory leaks in PostgreSQL's authentication system, as compiled regular expressions can hold significant system resources. The function follows the pattern of checking for the presence of resources before attempting to free them.

Note that the function only explicitly frees the regular expression - the AuthToken structure itself and its string data are typically managed by PostgreSQL's memory context system and will be freed when the appropriate memory context is destroyed.

## Parameters / Member Variables
- `token`: Pointer to the AuthToken structure to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - token_has_regexp (checks if the token contains a compiled regular expression)
  - pg_regfree (PostgreSQL's regex cleanup function)
  - AuthToken (the struct type being processed)
- Called from (representative examples):
  - check_ident_usermap (in src/backend/libpq/hba.c)

## Notes and Other Information
- This is a static function, only visible within the hba.c file
- The function only handles explicit cleanup of regex resources; the AuthToken struct itself is typically managed by memory contexts
- Part of PostgreSQL's HBA authentication system
- The function is designed to be safe to call even if the token doesn't contain a regular expression
- Regular expressions in AuthTokens are used for pattern matching in authentication rules
- Proper resource cleanup is essential to prevent memory leaks in long-running server processes