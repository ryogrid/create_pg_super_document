# aclmembers

## Location
src/backend/utils/adt/acl.c: 1540 - 1591

## Overview
Extracts all role IDs mentioned in an Access Control List, returning them as a sorted array of unique OIDs without distinguishing between grantors and grantees.

## Definition


## Detailed Description
The  function analyzes an ACL to collect all role IDs that appear in any capacity - either as grantees (recipients of privileges) or grantors (those who granted privileges). The function does not distinguish between these roles, simply collecting all mentioned role OIDs.

The function excludes the special PUBLIC role ID () from the results since it's not a real role but a system-wide placeholder. The returned array is sorted and deduplicated for efficient processing by calling code.

This function is commonly used in dependency tracking scenarios where the system needs to know which roles are referenced by an ACL before making changes to role membership or when processing GRANT/REVOKE operations.

## Parameters / Member Variables
- : The Access Control List to examine for role references
- : Output parameter - pointer to receive the allocated array of role OIDs

## Dependencies
- Functions called/Symbols referenced:
  - check_acl
  - ACL_NUM
  - ACL_DAT
  - ACL_ID_PUBLIC
  - palloc
  - qsort
  - oid_cmp
  - qunique
- Called from (representative examples):
  - SetDefaultACL
  - ExecGrant_Attribute
  - ExecGrant_Relation
  - ExecGrant_common
  - recordDependencyOnNewAcl
  - ReplaceRoleInInitPriv

## Notes and Other Information
- Returns 0 and sets  to NULL if ACL is NULL or empty
- Allocates worst-case space (2 * ACL_NUM entries) but doesn't shrink after deduplication
- Uses  with  for sorting and  for deduplication
- Memory allocated for the role array is caller's responsibility to free
- Both grantee and grantor roles are collected, though grantor is typically never PUBLIC
- Critical for ACL dependency tracking and role management operations