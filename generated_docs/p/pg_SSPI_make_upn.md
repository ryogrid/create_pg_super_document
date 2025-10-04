# pg_SSPI_make_upn

## Location
[src/backend/libpq/auth.c:1493-1596](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L1493-L1596)

## Overview
Converts Windows SAM-compatible domain and account names to Kerberos User Principal Name (UPN) format for SSPI authentication in PostgreSQL.

## Definition
```c
static int pg_SSPI_make_upn(char *accountname, size_t accountnamesize, 
                           char *domainname, size_t domainnamesize, 
                           bool update_accountname)
```

## Detailed Description
pg_SSPI_make_upn performs name translation from Windows SAM-compatible format (DOMAIN\\username) to Kerberos User Principal Name format (username@realm.name). This conversion is essential for proper Kerberos authentication in mixed Windows/Active Directory environments.

The function works by:
1. **SAM Name Construction**: Building a SAM-compatible name in the format "DOMAIN\\username"
2. **UPN Translation**: Using the Windows TranslateName API to convert from NameSamCompatible to NameUserPrincipal format
3. **Realm Extraction**: Parsing the resulting UPN to extract the Kerberos realm name (the part after '@')
4. **Buffer Management**: Safely copying the extracted realm and optionally the translated username back to the provided buffers, with proper size validation

The function includes comprehensive error handling for various failure scenarios including translation failures, buffer size violations, and malformed UPN results.

## Parameters / Member Variables
- : Input/output buffer containing the Windows account name; may be updated with Kerberos username if update_accountname is true
- : Size of the accountname buffer to prevent buffer overflows
- localdomain: Input/output buffer containing the Windows domain name; replaced with Kerberos realm name on success
- : Size of the domainname buffer to prevent buffer overflows  
- : Boolean flag indicating whether to update the accountname with the Kerberos username from UPN (handles cases where UPN username differs from SAM username)

## Dependencies
- Functions called/Symbols referenced:
  - TranslateName (Windows API)
  - GetLastError (Windows API)
  - [psprintf](psprintf.md) (PostgreSQL)
  - [palloc](palloc.md)/pfree (PostgreSQL memory management)
  - strchr (C standard library)
  - strcpy (C standard library)
  - ereport (PostgreSQL error reporting)
  - STATUS_OK
  - STATUS_ERROR
- Called from (representative examples):
  - [pg_SSPI_recvauth](pg_SSPI_recvauth.md)
  - LDAP_OPT_DIAGNOSTIC_MESSAGE context

## Notes and Other Information
- This function is Windows-specific and only available in Windows builds of PostgreSQL
- The realm name returned by Windows is typically in lowercase, which is acceptable since SSPI authentication comparisons are case-insensitive
- Handles the case where UPN and SAM usernames may differ in Active Directory environments
- Includes buffer overflow protection by validating sizes before copying translated names
- Uses PostgreSQL's memory allocation functions (palloc/pfree) for temporary buffers
- Returns STATUS_OK on success or STATUS_ERROR on any failure
- All errors are logged at LOG level with appropriate error codes
- The function modifies the input buffers in-place, making it destructive to the original domain/account names
- Essential for proper Kerberos realm resolution in complex Active Directory topologies

## Simplified Source

```c
// Simplified version of pg_SSPI_make_upn
static int pg_SSPI_make_upn(char *accountname, size_t accountnamesize,
                           char *domainname, size_t domainnamesize,
                           bool update_accountname) {
    // Step 1: Build SAM-compatible name (DOMAIN\username)
    char *samname = psprintf("%s\\%s", domainname, accountname);

    // Step 2: Get required buffer size for UPN translation
    ULONG upnamesize = 0;
    BOOLEAN result = TranslateName(samname, NameSamCompatible, NameUserPrincipal,
                                  NULL, &upnamesize);

    if (!result && GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        pfree(samname);
        ereport(LOG, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                     errmsg("could not translate name")));
        return STATUS_ERROR;
    }

    // Step 3: Allocate buffer and perform actual translation
    char *upname = palloc(upnamesize);
    result = TranslateName(samname, NameSamCompatible, NameUserPrincipal,
                          upname, &upnamesize);
    pfree(samname);

    if (!result) {
        pfree(upname);
        ereport(LOG, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                     errmsg("could not translate name")));
        return STATUS_ERROR;
    }

    // Step 4: Find '@' separator in UPN (username@realm.name)
    char *realm_start = strchr(upname, '@');
    if (!realm_start) {
        pfree(upname);
        ereport(LOG, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                     errmsg("could not translate name")));
        return STATUS_ERROR;
    }

    // Step 5: Extract and validate realm name length
    size_t realm_length = upnamesize - (realm_start - upname + 1);
    if (realm_length > domainnamesize) {
        pfree(upname);
        ereport(LOG, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                     errmsg("realm name too long")));
        return STATUS_ERROR;
    }

    // Step 6: Replace domain name with Kerberos realm
    strcpy(domainname, realm_start + 1);

    // Step 7: Optionally update account name with UPN username
    if (update_accountname) {
        size_t username_length = realm_start - upname + 1;
        if (username_length > accountnamesize) {
            pfree(upname);
            ereport(LOG, (errcode(ERRCODE_INVALID_ROLE_SPECIFICATION),
                         errmsg("translated account name too long")));
            return STATUS_ERROR;
        }

        *realm_start = '\0';  // Null-terminate username part
        strcpy(accountname, upname);
    }

    pfree(upname);
    return STATUS_OK;
}
```