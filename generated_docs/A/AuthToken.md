# AuthToken

## Location
[src/include/libpq/hba.h:87-92](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/hba.h#L87-L92)

## Overview
AuthToken is a structure that represents a single string token lexed from authentication configuration files (pg_ident.conf or pg_hba.conf), including metadata about whether the token was quoted and optional regex compilation.

## Definition


## Detailed Description
AuthToken serves as the fundamental building block for parsing PostgreSQL authentication configuration files. Each token extracted from pg_hba.conf or pg_ident.conf is represented by this structure, which not only stores the string value but also preserves important parsing context such as whether the original token was quoted in the configuration file. The structure also supports regular expression functionality - when a token string begins with a slash, it may contain a regular expression pattern that gets compiled and stored in the regex field for pattern matching operations.

## Parameters / Member Variables
- : The actual string content of the token as extracted from the configuration file
- : Boolean flag indicating whether the original token was enclosed in quotes in the configuration file
- : Pointer to compiled regular expression structure when the token contains a regex pattern (typically when string begins with '/')

## Dependencies
- Functions called/Symbols referenced:
  - regex_t (POSIX regex type)
- Called from (representative examples):
  - token_matches_insensitive
  - [next_token](../n/next_token.md)
  - [make_auth_token](../m/make_auth_token.md)
  - [free_auth_token](../f/free_auth_token.md)
  - [copy_auth_token](../c/copy_auth_token.md)
  - [regcomp_auth_token](../r/regcomp_auth_token.md)
  - [regexec_auth_token](../r/regexec_auth_token.md)
  - [tokenize_expand_file](../t/tokenize_expand_file.md)
  - [tokenize_auth_file](../t/tokenize_auth_file.md)
  - [check_role](../c/check_role.md)
  - check_db
  - [parse_hba_line](../p/parse_hba_line.md)
  - [parse_ident_line](../p/parse_ident_line.md)
  - [check_ident_usermap](../c/check_ident_usermap.md)
  - [fill_hba_line](../f/fill_hba_line.md)

## Notes and Other Information
- This structure is central to PostgreSQL's authentication configuration parsing system
- The regex functionality is particularly important for pattern matching in identity mapping configurations
- Memory management functions (make_auth_token, free_auth_token, copy_auth_token) handle proper allocation and cleanup
- The quoted flag helps preserve the original semantics of configuration entries during parsing and validation
- Used extensively throughout the HBA (Host-Based Authentication) subsystem