# aclmask

## Location
src/backend/utils/adt/acl.c: 1388 - 1476

## Overview
Computes the bitmask of all privileges held by a given role ID according to an Access Control List (ACL), providing flexible querying modes for privilege checking.

## Definition


## Detailed Description
The  function is the core privilege checking function in PostgreSQL's Access Control List system. It determines what privileges a given role has by examining both direct grants and indirect grants through role membership. The function supports two operational modes:  for checking if all specified privileges are held, and  for early exit when any privilege is found.

The function performs a two-phase privilege check:
1. **Direct privileges**: Checks privileges granted directly to the role or to PUBLIC
2. **Indirect privileges**: Checks privileges granted through role membership (inheritance)

Owner privileges are handled specially - owners implicitly have all grant options for their objects.

## Parameters / Member Variables
- : The Access Control List to examine for privileges
- : The OID of the role whose privileges are being checked
- : The OID of the object owner (for implicit owner privileges)
- : Bitmask specifying which privileges to check for
- : Query mode -  (check all privileges) or  (early exit on any match)

## Dependencies
- Functions called/Symbols referenced:
  - check_acl
  - has_privs_of_role
  - ACL_NUM
  - ACL_DAT
  - ACLITEM_ALL_GOPTION_BITS
  - ACL_ID_PUBLIC
  - ACLMASK_ALL
- Called from (representative examples):
  - object_aclmask_ext
  - pg_attribute_aclmask_ext
  - pg_class_aclmask_ext
  - pg_namespace_aclmask_ext
  - LockTableAclCheck

## Notes and Other Information
- Returns 0 immediately if mask is 0 (no privileges requested)
- Throws ERROR if ACL is NULL (should not happen with proper default ACL insertion)
- Optimizes by checking direct grants first, then indirect grants only for remaining privileges
- Usage patterns include checking for any privileges (), all privileges (), or determining exact privileges held
- Critical for PostgreSQL's security model, used throughout the system for access control decisions