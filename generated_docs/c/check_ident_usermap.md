# check_ident_usermap

## Location
src/backend/libpq/hba.c: 2757 - 2903

## Overview
Evaluates a single identity mapping rule from pg_ident.conf to determine if a system user can be mapped to a PostgreSQL role, supporting both literal matching and regular expression patterns with substitution.

## Definition


## Detailed Description
This function implements the core logic for PostgreSQL's user identity mapping system used by authentication methods like ident, peer, GSSAPI, SSPI, and cert. It processes individual mapping rules to determine if a system-authenticated user should be allowed to connect as a specific PostgreSQL role.

The function supports two matching modes:

**Literal Matching**: Direct string comparison between the system username and the pattern in the identity mapping rule. Case sensitivity is controlled by the case_insensitive parameter.

**Regular Expression Matching**: When the system_user pattern begins with '/', it's treated as a regular expression. The function:
1. Executes the regex against the provided system username
2. Captures subgroups from the match
3. Performs substitution of \1 in the PostgreSQL username pattern with the first captured group
4. Validates that substitution requirements are met

The function also performs comprehensive error checking for regex compilation errors and invalid substitution patterns.

## Parameters / Member Variables
- : Pointer to IdentLine structure containing the parsed mapping rule
- : Name of the user map being searched (must match identLine->usermap)
- : PostgreSQL role name being requested
- : System username from authentication (ident, peer, etc.)
- : Whether matching should be case-insensitive
- : Output parameter set to true if mapping succeeds
- : Output parameter set to true if error occurs during processing

## Dependencies
- Functions called/Symbols referenced:
  - get_role_oid (converts role name to OID)
  - token_has_regexp (checks if token contains regex pattern)
  - [regexec_auth_token](../r/regexec_auth_token.md) (executes regex against input string)
  - [pg_regerror](../p/pg_regerror.md) (converts regex error codes to strings)
  - token_is_member_check/token_matches/token_matches_insensitive (token matching)
  - [make_auth_token](../m/make_auth_token.md)/free_auth_token (auth token management)
  - [check_role](check_role.md) (validates role membership and permissions)
  - list_make1 (creates single-element list)
  - [palloc0](../p/palloc0.md)/pfree (memory management)
  - ereport/errcode/errmsg (error reporting)
  - Data structures: IdentLine, AuthToken, regmatch_t
- Called from:
  - [check_usermap](check_usermap.md) (src/backend/libpq/hba.c:2935)

## Notes and Other Information
- Returns results through output parameters found_p and error_p rather than return value
- Supports regex backreference substitution with \1 pattern in PostgreSQL username
- Performs early exit if usermap name doesn't match the rule being evaluated
- Regular expressions must be compiled during parse_ident_line() before this function is called
- The function handles both simple string matching and complex regex-based transformations
- Critical component of PostgreSQL's external authentication integration
- Regex substitution allows dynamic username mapping based on system username patterns (e.g., domain\user -> user)