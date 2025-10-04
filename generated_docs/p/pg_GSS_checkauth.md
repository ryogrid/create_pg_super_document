# pg_GSS_checkauth

## Location
[src/backend/libpq/auth.c:1081-1187](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1081-L1187)

## Overview
pg_GSS_checkauth validates that a GSSAPI-authenticated user is authorized to connect as the requested database user, performing principal name extraction, realm verification, and user mapping.

## Definition

```c
structively modify it here to remove the
		 * realm. Then advance past the separator to check the realm.
		 */
		if (!port->hba->include_realm)
			*cp = '\0';
```
## Detailed Description
pg_GSS_checkauth performs the final authorization phase of GSSAPI authentication after the security context has been successfully established. It extracts the authenticated principal name from the GSS context, validates the Kerberos realm against configured requirements, handles realm inclusion/exclusion based on HBA configuration, and performs user mapping to determine if the authenticated principal is authorized to connect as the requested PostgreSQL user. The function sets the authenticated identity and stores the original principal name for logging and display purposes.

## Parameters / Member Variables
- : Connection port structure containing client connection information, GSS authentication state, and HBA configuration

## Dependencies
- Functions called/Symbols referenced:
  - gss_display_name (extracts principal name from GSS context)
  - gss_release_buffer (releases GSS buffer resources)
  - [palloc](palloc.md) (allocates memory for principal name)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (stores principal in TopMemoryContext)
  - [set_authn_id](../s/set_authn_id.md) (sets authenticated identity for the connection)
  - strchr (locates realm separator '@' in principal name)
  - [pg_strcasecmp](pg_strcasecmp.md)/strcmp (case-sensitive/insensitive realm comparison)
  - [check_usermap](../c/check_usermap.md) (validates user mapping authorization)
  - [pg_GSS_error](pg_GSS_error.md) (error reporting for GSS failures)
  - [pfree](pfree.md) (memory cleanup)
- Called from (representative examples):
  - [pg_GSS_recvauth](pg_GSS_recvauth.md) function in auth.c:1073
  - [ClientAuthentication](../C/ClientAuthentication.md) function in auth.c:566

## Notes and Other Information
- Handles both case-sensitive and case-insensitive realm matching based on pg_krb_caseins_users
- Supports optional realm inclusion in username via include_realm HBA option
- Validates GSS principal realm against configured krb_realm in HBA
- Returns STATUS_ERROR for realm mismatches or missing realms when required
- Sets authenticated identity immediately upon successful GSS name extraction
- Stores original principal name in backend memory for later display
- Delegates final authorization to check_usermap function
- Properly handles null-termination of GSS buffer content
- Part of the two-phase GSSAPI authentication: context establishment (pg_GSS_recvauth) and authorization (pg_GSS_checkauth)

## Simplified Source

```c
static int
pg_GSS_checkauth(Port *port)
{
    int ret;
    OM_uint32 maj_stat, min_stat, lmin_s;
    gss_buffer_desc gbuf;
    char *princ;

    // Extract authenticated principal name from GSS context
    maj_stat = gss_display_name(&min_stat, port->gss->name, &gbuf, NULL);
    if (maj_stat != GSS_S_COMPLETE) {
        pg_GSS_error("retrieving GSS user name failed", maj_stat, min_stat);
        return STATUS_ERROR;
    }

    // Convert GSS buffer to null-terminated string
    princ = palloc(gbuf.length + 1);
    memcpy(princ, gbuf.value, gbuf.length);
    princ[gbuf.length] = '\0';
    gss_release_buffer(&lmin_s, &gbuf);

    // Store principal name and set authenticated identity
    port->gss->princ = MemoryContextStrdup(TopMemoryContext, princ);
    set_authn_id(port, princ);

    // Handle realm separation and validation
    if (strchr(princ, '@')) {
        char *cp = strchr(princ, '@');

        // Remove realm from username if not including it
        if (!port->hba->include_realm)
            *cp = '\0';
        cp++;

        // Validate realm if configured
        if (port->hba->krb_realm != NULL && strlen(port->hba->krb_realm)) {
            if (pg_krb_caseins_users)
                ret = pg_strcasecmp(port->hba->krb_realm, cp);
            else
                ret = strcmp(port->hba->krb_realm, cp);

            if (ret) {
                elog(DEBUG2, "GSSAPI realm (%s) and configured realm (%s) don't match",
                     cp, port->hba->krb_realm);
                pfree(princ);
                return STATUS_ERROR;
            }
        }
    } else if (port->hba->krb_realm && strlen(port->hba->krb_realm)) {
        elog(DEBUG2, "GSSAPI did not return realm but realm matching was requested");
        pfree(princ);
        return STATUS_ERROR;
    }

    // Perform user mapping authorization
    ret = check_usermap(port->hba->usermap, port->user_name, princ, pg_krb_caseins_users);

    pfree(princ);
    return ret;
}
```