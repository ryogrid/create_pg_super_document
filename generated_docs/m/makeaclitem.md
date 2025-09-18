# makeaclitem

## Location
src/backend/utils/adt/acl.c: 1634 - 1686

## Overview
Creates a new ACL item from individual components including grantee, grantor, privilege string, and grant option flag.

## Definition
```c
Datum makeaclitem(PG_FUNCTION_ARGS)
```

## Detailed Description
The `makeaclitem` function constructs a new AclItem structure from its constituent parts. It takes a grantee OID, grantor OID, a text string representing privileges, and a boolean indicating whether the grant option should be set. The function parses the privilege string using a predefined mapping table that translates human-readable privilege names (like "SELECT", "INSERT", etc.) into their corresponding ACL bit masks.

The function uses a static privilege mapping table that covers all standard PostgreSQL privileges including table privileges (SELECT, INSERT, UPDATE, DELETE), schema privileges (CREATE, USAGE), database privileges (CONNECT, CREATE), and system privileges (ALTER SYSTEM). It also handles legacy privileges like "RULE" by ignoring them.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS[0]` (Oid): The grantee - OID of the role receiving the privileges
- `PG_FUNCTION_ARGS[1]` (Oid): The grantor - OID of the role granting the privileges  
- `PG_FUNCTION_ARGS[2]` (text*): Text string containing comma-separated privilege names
- `PG_FUNCTION_ARGS[3]` (bool): Grant option flag - whether grantee can grant these privileges to others

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_OID (macro for extracting OID arguments)
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - PG_GETARG_BOOL (macro for extracting boolean argument)
  - [convert_any_priv_string](../c/convert_any_priv_string.md) (parses privilege string using mapping table)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation)
  - ACLITEM_SET_PRIVS_GOPTIONS (macro to set privileges and grant options)
  - PG_RETURN_ACLITEM_P (macro to return AclItem result)
  - Various ACL privilege constants (ACL_SELECT, ACL_INSERT, etc.)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a PostgreSQL SQL function for creating ACL items programmatically
- The privilege mapping supports both current and legacy privilege names
- The grant option parameter controls whether the grantee can further grant these privileges
- Uses a static privilege map for efficient string-to-privilege conversion
- Allocates memory for the result using PostgreSQL's memory management system
- Legacy "RULE" privileges are explicitly ignored (mapped to 0)