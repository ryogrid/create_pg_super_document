# aclparse

## Location
[src/backend/utils/adt/acl.c:270-425](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L270-L425)

## Overview
Parses a complete ACL specification string into an AclItem structure, handling role names, privileges, and grant options with backward compatibility support for group/user keywords.

## Definition

```c
static const char *
aclparse(const char *s, AclItem *aip, Node *escontext)
```
## Detailed Description
This function is the core parser for Access Control List specifications in PostgreSQL. It processes ACL strings of the format  where privileges are represented by single characters (r, w, a, etc.) and the asterisk (*) indicates grant option privileges.

The parser handles optional group/user keywords for backward compatibility with older PostgreSQL versions, though these keywords are no longer necessary in the roles-based system. It processes each privilege character, building up privilege and grant option bitmasks, then resolves role names to OIDs. If no grantor is specified, it defaults to the bootstrap superuser with a warning.

The function supports comprehensive error handling through the escontext mechanism and validates that all referenced roles exist in the system catalog.

## Parameters / Member Variables
- `*s`: Input string containing the ACL specification to parse
- `*aip`: Pointer to AclItem structure to populate with parsed values (grantee, grantor, privileges, grant options)
- `*escontext`: Error context node - if ErrorSaveData, errors are logged rather than thrown immediately
## Dependencies
- Functions called/Symbols referenced:
  - [getid](../g/getid.md) (extracts identifiers from input string)
  - [get_role_oid](../g/get_role_oid.md) (resolves role names to OIDs)
  - ereturn (error handling macro)
  - ACLITEM_SET_PRIVS_GOPTIONS (macro to set privileges and grant options)
  - Various ACL constants (ACL_INSERT, ACL_SELECT, etc.)
  - ACL_ID_PUBLIC (special OID for public role)
  - BOOTSTRAP_SUPERUSERID (default grantor OID)
- Called from (representative examples):
  - [aclitemin](aclitemin.md) (during ACL input conversion)

## Notes and Other Information
The parser supports all PostgreSQL privilege types including INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, EXECUTE, USAGE, CREATE, CREATE_TEMP, CONNECT, SET, ALTER_SYSTEM, and MAINTAIN. It maintains backward compatibility by accepting the obsolete 'R' (RULE) privilege character while ignoring it. The grantor specification after '/' is optional but recommended to avoid the deprecation warning about defaulting to the superuser.

## Simplified Source

```c
static const char *
aclparse(const char *s, AclItem *aip, Node *escontext)
{
    AclMode privs, goption, read;
    char name[NAMEDATALEN];
    char name2[NAMEDATALEN];

    // Parse first identifier (could be keyword or role name)
    s = getid(s, name, escontext);
    if (s == NULL)
        return NULL;

    // Handle optional "group" or "user" keywords
    if (*s != '=') {
        if (strcmp(name, "group") != 0 && strcmp(name, "user") != 0)
            ereturn(escontext, NULL, "unrecognized key word");

        // Get actual name after keyword
        s = getid(s, name, escontext);
        if (s == NULL || name[0] == '\0')
            ereturn(escontext, NULL, "missing name after keyword");
    }

    // Expect '=' sign
    if (*s != '=')
        ereturn(escontext, NULL, "missing '=' sign");

    // Parse privilege characters
    privs = goption = ACL_NO_RIGHTS;
    for (++s, read = 0; isalpha(*s) || *s == '*'; s++) {
        switch (*s) {
            case '*':
                goption |= read;  // Grant option for previous privilege
                break;
            case ACL_INSERT_CHR:
                read = ACL_INSERT;
                break;
            case ACL_SELECT_CHR:
                read = ACL_SELECT;
                break;
            case ACL_UPDATE_CHR:
                read = ACL_UPDATE;
                break;
            case ACL_DELETE_CHR:
                read = ACL_DELETE;
                break;
            case ACL_TRUNCATE_CHR:
                read = ACL_TRUNCATE;
                break;
            case ACL_REFERENCES_CHR:
                read = ACL_REFERENCES;
                break;
            case ACL_TRIGGER_CHR:
                read = ACL_TRIGGER;
                break;
            case ACL_EXECUTE_CHR:
                read = ACL_EXECUTE;
                break;
            case ACL_USAGE_CHR:
                read = ACL_USAGE;
                break;
            case ACL_CREATE_CHR:
                read = ACL_CREATE;
                break;
            case ACL_CREATE_TEMP_CHR:
                read = ACL_CREATE_TEMP;
                break;
            case ACL_CONNECT_CHR:
                read = ACL_CONNECT;
                break;
            case ACL_SET_CHR:
                read = ACL_SET;
                break;
            case ACL_ALTER_SYSTEM_CHR:
                read = ACL_ALTER_SYSTEM;
                break;
            case ACL_MAINTAIN_CHR:
                read = ACL_MAINTAIN;
                break;
            case 'R':
                read = 0;  // Ignore old RULE privileges
                break;
            default:
                ereturn(escontext, NULL, "invalid privilege character");
        }
        privs |= read;
    }

    // Resolve grantee role
    if (name[0] == '\0')
        aip->ai_grantee = ACL_ID_PUBLIC;
    else {
        aip->ai_grantee = get_role_oid(name, true);
        if (!OidIsValid(aip->ai_grantee))
            ereturn(escontext, NULL, "role does not exist");
    }

    // Parse optional grantor after '/'
    if (*s == '/') {
        s = getid(s + 1, name2, escontext);
        if (s == NULL || name2[0] == '\0')
            ereturn(escontext, NULL, "missing name after '/'");

        aip->ai_grantor = get_role_oid(name2, true);
        if (!OidIsValid(aip->ai_grantor))
            ereturn(escontext, NULL, "grantor role does not exist");
    } else {
        // Default to bootstrap superuser with warning
        aip->ai_grantor = BOOTSTRAP_SUPERUSERID;
        ereport(WARNING, "defaulting grantor to superuser");
    }

    // Set privileges and grant options in ACL item
    ACLITEM_SET_PRIVS_GOPTIONS(*aip, privs, goption);

    return s;
}
```