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
- : Input string containing the ACL specification to parse
- : Pointer to AclItem structure to populate with parsed values (grantee, grantor, privileges, grant options)
- : Error context node - if ErrorSaveData, errors are logged rather than thrown immediately

## Dependencies
- Functions called/Symbols referenced:
  - [getid](../g/getid.md) (extracts identifiers from input string)
  - get_role_oid (resolves role names to OIDs)
  - ereturn (error handling macro)
  - ACLITEM_SET_PRIVS_GOPTIONS (macro to set privileges and grant options)
  - Various ACL constants (ACL_INSERT, ACL_SELECT, etc.)
  - ACL_ID_PUBLIC (special OID for public role)
  - BOOTSTRAP_SUPERUSERID (default grantor OID)
- Called from (representative examples):
  - [aclitemin](aclitemin.md) (during ACL input conversion)

## Notes and Other Information
The parser supports all PostgreSQL privilege types including INSERT, SELECT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, EXECUTE, USAGE, CREATE, CREATE_TEMP, CONNECT, SET, ALTER_SYSTEM, and MAINTAIN. It maintains backward compatibility by accepting the obsolete 'R' (RULE) privilege character while ignoring it. The grantor specification after '/' is optional but recommended to avoid the deprecation warning about defaulting to the superuser.