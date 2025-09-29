# check_db

## Location
[src/backend/libpq/hba.c:987-1030](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L987-L1030)

## Overview
Validates whether a database/role combination matches a list of authentication tokens from HBA (Host-Based Authentication) configuration entries.

## Definition

```c
static bool
check_db(const char *dbname, const char *role, Oid roleid, List *tokens)
```
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
  - [is_member](../i/is_member.md)
  - token_has_regexp
  - [regexec_auth_token](../r/regexec_auth_token.md)
  - token_matches
- Called from (representative examples):
  - [check_hba](check_hba.md) (in hba.c)

## Notes and Other Information
- Part of PostgreSQL's HBA authentication framework
- Handles special WAL sender connection logic for replication
- Supports both keyword matching and regular expression patterns
- Returns true on first match found, false if no tokens match
- Keywords are processed before regular expressions for efficiency
- "samegroup" and "samerole" are treated identically (legacy compatibility)

## Simplified Source

```c
static bool
check_db(const char *dbname, const char *role, Oid roleid, List *tokens)
{
    ListCell *cell;
    AuthToken *tok;

    // Check each authentication token
    foreach(cell, tokens)
    {
        tok = lfirst(cell);

        // Special handling for WAL sender connections
        if (am_walsender && !am_db_walsender)
        {
            // Physical replication can only match "replication" keyword
            if (token_is_keyword(tok, "replication"))
                return true;
        }
        else if (token_is_keyword(tok, "all"))
            return true;
        else if (token_is_keyword(tok, "sameuser"))
        {
            // Database name must match role name
            if (strcmp(dbname, role) == 0)
                return true;
        }
        else if (token_is_keyword(tok, "samegroup") ||
                 token_is_keyword(tok, "samerole"))
        {
            // Role must be member of group/role with database name
            if (is_member(roleid, dbname))
                return true;
        }
        else if (token_is_keyword(tok, "replication"))
            continue;  // Skip if not a walsender
        else if (token_has_regexp(tok))
        {
            // Regular expression matching
            if (regexec_auth_token(dbname, tok, 0, NULL) == REG_OKAY)
                return true;
        }
        else if (token_matches(tok, dbname))
            return true;  // Exact string match
    }
    return false;
}
```