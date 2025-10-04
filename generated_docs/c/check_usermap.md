# check_usermap

## Location
[src/backend/libpq/hba.c:2904-2958](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L2904-L2958)

## Overview
Validates whether a system user is authorized to connect as a specified PostgreSQL user according to a usermap configuration, supporting both implicit sameuser mapping and explicit usermap file lookups.

## Definition

```c
int
check_usermap(const char *usermap_name,
			  const char *pg_user,
			  const char *system_user,
			  bool case_insensitive)
```
## Detailed Description
The `check_usermap` function is a core authentication component that validates user mapping in PostgreSQL. It handles two distinct scenarios:

1. **Implicit mapping (sameuser/samerole)**: When `usermap_name` is NULL or empty, the function performs a direct comparison between `pg_user` and `system_user`. This is the traditional "sameuser" authentication mode where the system user must exactly match the PostgreSQL user.

2. **Explicit usermap lookup**: When a usermap name is provided, the function searches through the parsed identity mapping file (`parsed_ident_lines`) to find matching entries. It delegates the actual matching logic to `check_ident_usermap` for each line in the configuration.

The function supports both case-sensitive and case-insensitive matching modes. It returns STATUS_OK for successful authorization or STATUS_ERROR for failures, logging appropriate error messages for troubleshooting.

## Parameters / Member Variables
- `usermap_name`: Name of the usermap to check against; NULL or empty string triggers implicit sameuser mode
- `pg_user`: The PostgreSQL username that the client wants to connect as
- `system_user`: The authenticated system username (from OS, Kerberos, etc.)
- `case_insensitive`: Boolean flag controlling whether username comparisons should ignore case

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strcasecmp](../p/pg_strcasecmp.md) (for case-insensitive string comparison)
  - strcmp (for case-sensitive string comparison)  
  - ereport (for error logging)
  - [check_ident_usermap](check_ident_usermap.md) (for processing usermap file entries)
  - parsed_ident_lines (global list of parsed identity mapping entries)
  - STATUS_OK, STATUS_ERROR (return status constants)
- Called from (representative examples):
  - [pg_GSS_checkauth](../p/pg_GSS_checkauth.md) (GSS/Kerberos authentication)
  - [pg_SSPI_recvauth](../p/pg_SSPI_recvauth.md) (Windows SSPI authentication)
  - [ident_inet](../i/ident_inet.md) (ident protocol authentication)
  - [auth_peer](../a/auth_peer.md) (peer authentication)
  - [CheckCertAuth](../C/CheckCertAuth.md) (SSL certificate authentication)

## Notes and Other Information
- This function is located at src/backend/libpq/hba.c:2904-2958
- The parsed_ident_lines global variable must be populated by load_ident() before this function can process explicit usermaps
- Error messages are logged at LOG level to help administrators debug authentication issues
- The function implements PostgreSQL's user name mapping feature, which is essential for environments where system usernames differ from database usernames
- Regular expression and group membership features are handled by the check_ident_usermap helper function

## Simplified Source

```c
int check_usermap(const char *usermap_name,
                 const char *pg_user,
                 const char *system_user,
                 bool case_insensitive)
{
    bool found_entry = false, error = false;

    // Handle implicit sameuser mapping (NULL or empty usermap name)
    if (usermap_name == NULL || usermap_name[0] == '\0') {
        // Direct comparison between pg_user and system_user
        if (case_insensitive) {
            if (pg_strcasecmp(pg_user, system_user) == 0)
                return STATUS_OK;
        } else {
            if (strcmp(pg_user, system_user) == 0)
                return STATUS_OK;
        }

        // Log mismatch for sameuser mode
        ereport(LOG, (errmsg("provided user name (%s) and authenticated user name (%s) do not match",
                             pg_user, system_user)));
        return STATUS_ERROR;
    }

    // Handle explicit usermap lookup
    ListCell *line_cell;
    foreach(line_cell, parsed_ident_lines) {
        check_ident_usermap(lfirst(line_cell), usermap_name,
                           pg_user, system_user, case_insensitive,
                           &found_entry, &error);
        if (found_entry || error)
            break;
    }

    // Log if no matching entry found in usermap
    if (!found_entry && !error) {
        ereport(LOG, (errmsg("no match in usermap \"%s\" for user \"%s\" authenticated as \"%s\"",
                             usermap_name, pg_user, system_user)));
    }

    return found_entry ? STATUS_OK : STATUS_ERROR;
}
```