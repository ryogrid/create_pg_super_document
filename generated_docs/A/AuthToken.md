# AuthToken

## Location
src/include/libpq/hba.h: 87 - 92

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
  - next_token
  - make_auth_token
  - free_auth_token
  - copy_auth_token
  - regcomp_auth_token
  - regexec_auth_token
  - tokenize_expand_file
  - tokenize_auth_file
  - check_role
  - check_db
  - parse_hba_line
  - parse_ident_line
  - check_ident_usermap
  - fill_hba_line

## Notes and Other Information
- This structure is central to PostgreSQL's authentication configuration parsing system
- The regex functionality is particularly important for pattern matching in identity mapping configurations
- Memory management functions (make_auth_token, free_auth_token, copy_auth_token) handle proper allocation and cleanup
- The quoted flag helps preserve the original semantics of configuration entries during parsing and validation
- Used extensively throughout the HBA (Host-Based Authentication) subsystem