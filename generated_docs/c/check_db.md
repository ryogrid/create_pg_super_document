# check_db

## Location
src/backend/libpq/hba.c: 987 - 1030

## Overview
Validates whether a database/role combination matches a list of authentication tokens from HBA (Host-Based Authentication) configuration entries.

## Definition


## Detailed Description
The  function is a core component of PostgreSQL's Host-Based Authentication (HBA) system that determines if a given database and role combination matches any of the database specifications in an HBA entry. It processes a list of AuthToken objects, checking each one sequentially until a match is found.

The function handles several special keywords and patterns:
- "all": matches any database
- "sameuser": matches when database name equals role name
- "samegroup"/"samerole": matches when the role is a member of a group/role with the database name
- "replication": special handling for physical replication connections
- Regular expressions: pattern matching against database names
- Exact string matching for specific database names

Special logic exists for WAL sender connections, where physical replication connections can only match the "replication" keyword.

## Parameters / Member Variables
- : The name of the database being accessed
- : The name of the role attempting access
- : The OID of the role attempting access
- : List of AuthToken objects representing database specifications from HBA entry

## Dependencies
- Functions called/Symbols referenced:
  - token_is_keyword
  - is_member
  - token_has_regexp
  - regexec_auth_token
  - token_matches
- Called from (representative examples):
  - check_hba (in hba.c)

## Notes and Other Information
- Part of PostgreSQL's HBA authentication framework
- Handles special WAL sender connection logic for replication
- Supports both keyword matching and regular expression patterns
- Returns true on first match found, false if no tokens match
- Keywords are processed before regular expressions for efficiency
- "samegroup" and "samerole" are treated identically (legacy compatibility)