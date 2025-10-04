# check_ident_usermap

## Location
[src/backend/libpq/hba.c:2757-2903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2757-L2903)

## Overview
Evaluates a single identity mapping rule from pg_ident.conf to determine if a system user can be mapped to a PostgreSQL role, supporting both literal matching and regular expression patterns with substitution.

## Definition

```c
static void
check_ident_usermap(IdentLine *identLine, const char *usermap_name,
					const char *pg_user, const char *system_user,
					bool case_insensitive, bool *found_p, bool *error_p)
```
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
- `*identLine`: Pointer to IdentLine structure containing the parsed mapping rule
- `*usermap_name`: Name of the user map being searched (must match identLine->usermap)
- `*pg_user`: PostgreSQL role name being requested
- `*system_user`: System username from authentication (ident, peer, etc.)
- `case_insensitive`: Whether matching should be case-insensitive
- `*found_p`: Output parameter set to true if mapping succeeds
- `*error_p`: Output parameter set to true if error occurs during processing
## Dependencies
- Functions called/Symbols referenced:
  - [get_role_oid](../g/get_role_oid.md) (converts role name to OID)
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

## Simplified Source

```c
static void check_ident_usermap(IdentLine *identLine, const char *usermap_name,
                               const char *pg_user, const char *system_user,
                               bool case_insensitive, bool *found_p, bool *error_p) {
    Oid roleid;

    *found_p = false;
    *error_p = false;

    // Check if this line matches the requested usermap
    if (strcmp(identLine->usermap, usermap_name) != 0)
        return;

    // Get PostgreSQL role OID (no error if role doesn't exist)
    roleid = get_role_oid(pg_user, true);

    if (token_has_regexp(identLine->system_user)) {
        // Handle regular expression matching with substitution
        int r;
        regmatch_t matches[2];
        AuthToken *expanded_pg_user_token;
        bool created_temporary_token = false;

        // Execute regex against system username
        r = regexec_auth_token(system_user, identLine->system_user, 2, matches);
        if (r) {
            if (r != REG_NOMATCH) {
                // Report regex execution error
                char errstr[100];
                pg_regerror(r, identLine->system_user->regex, errstr, sizeof(errstr));
                ereport(LOG, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                             errmsg("regular expression match failed: %s", errstr)));
                *error_p = true;
            }
            return;
        }

        // Handle \1 substitution in PostgreSQL username
        char *ofs = strstr(identLine->pg_user->string, "\\1");
        if (!token_is_member_check(identLine->pg_user) &&
            !token_has_regexp(identLine->pg_user) &&
            ofs != NULL) {

            // Validate that we have a captured group for substitution
            if (matches[1].rm_so < 0) {
                ereport(LOG, (errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
                             errmsg("regular expression has no subexpressions for backreference")));
                *error_p = true;
                return;
            }

            // Build expanded username with substitution
            char *expanded_pg_user = palloc0(strlen(identLine->pg_user->string) - 2 +
                                           (matches[1].rm_eo - matches[1].rm_so) + 1);
            int offset = ofs - identLine->pg_user->string;

            // Copy parts: before \1, captured group, after \1
            memcpy(expanded_pg_user, identLine->pg_user->string, offset);
            memcpy(expanded_pg_user + offset,
                   system_user + matches[1].rm_so,
                   matches[1].rm_eo - matches[1].rm_so);
            strcat(expanded_pg_user, ofs + 2);

            // Create token for the expanded username
            expanded_pg_user_token = make_auth_token(expanded_pg_user, true);
            created_temporary_token = true;
            pfree(expanded_pg_user);
        } else {
            expanded_pg_user_token = identLine->pg_user;
        }

        // Check if PostgreSQL user matches
        *found_p = check_role(pg_user, roleid, list_make1(expanded_pg_user_token), case_insensitive);

        if (created_temporary_token)
            free_auth_token(expanded_pg_user_token);
    } else {
        // Handle literal string matching
        if (case_insensitive) {
            if (!token_matches_insensitive(identLine->system_user, system_user))
                return;
        } else {
            if (!token_matches(identLine->system_user, system_user))
                return;
        }

        // Check if PostgreSQL user matches
        *found_p = check_role(pg_user, roleid, list_make1(identLine->pg_user), case_insensitive);
    }
}
```