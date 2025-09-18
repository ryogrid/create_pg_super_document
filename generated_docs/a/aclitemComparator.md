# aclitemComparator

## Location
src/backend/utils/adt/acl.c: 724 - 747

## Overview
A static comparison function used by qsort to order AclItem structures in a canonical order for access control list processing.

## Definition


## Detailed Description
The aclitemComparator function implements a three-level hierarchical comparison for AclItem structures, used internally by the PostgreSQL ACL system to maintain a consistent ordering of access control entries. The function compares ACL items first by grantee (the entity receiving privileges), then by grantor (the entity granting privileges), and finally by the privilege bits themselves. This ordering ensures that ACL arrays are maintained in a canonical form, which is essential for efficient ACL operations like merging, searching, and deduplication.

The comparison follows a lexicographic ordering where:
1. Primary sort key: ai_grantee (recipient of privileges)
2. Secondary sort key: ai_grantor (granter of privileges) 
3. Tertiary sort key: ai_privs (privilege bitmask)

## Parameters / Member Variables
- : Pointer to the first AclItem to compare (cast from void*)
- : Pointer to the second AclItem to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - AclItem (structure type)
- Called from (representative examples):
  - aclitemsort (via qsort)

## Notes and Other Information
- This is a static function, only accessible within the acl.c compilation unit
- Returns standard qsort comparison values: -1 (less than), 0 (equal), 1 (greater than)
- The ordering is designed to group ACL entries by grantee first, making privilege lookups more efficient
- Uses simple integer comparison since ai_grantee, ai_grantor, and ai_privs are all numeric types (Oid and AclMode respectively)