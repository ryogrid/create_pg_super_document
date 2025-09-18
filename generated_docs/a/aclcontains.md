# aclcontains

## Location
[src/backend/utils/adt/acl.c:1612-1633](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L1612-L1633)

## Overview
Checks whether an Access Control List (ACL) contains a specific ACL item with matching grantee, grantor, and privileges.

## Definition


## Detailed Description
The  function determines if a given ACL contains an ACL item that matches the specified criteria. It performs an exact match on the grantee and grantor, and checks that all the privileges specified in the target ACL item are present in at least one item within the ACL. The function implements a contains operation where it verifies that the target item's privileges are a subset of an existing ACL entry's privileges.

The function iterates through all items in the ACL and returns true if it finds an entry where:
1. The grantee (user/role receiving privileges) matches exactly
2. The grantor (user/role granting privileges) matches exactly  
3. All privileges in the target item are present in the ACL entry (bitwise AND operation)

## Parameters / Member Variables
-  (Acl*): The Access Control List to search within
-  (AclItem*): The ACL item to search for

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ACL_P (macro for extracting ACL argument)
  - PG_GETARG_ACLITEM_P (macro for extracting AclItem argument)
  - [check_acl](../c/check_acl.md) (validates ACL structure)
  - ACL_NUM (macro to get number of ACL items)
  - ACL_DAT (macro to get ACL data array)
  - ACLITEM_GET_RIGHTS (macro to extract privilege bits from ACL item)
  - PG_RETURN_BOOL (macro to return boolean result)
- Called from (representative examples):
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- This function is exposed as a PostgreSQL SQL function for ACL operations
- The privilege matching uses bitwise operations to ensure all requested privileges are present
- Returns false if no matching ACL entry is found
- The function validates the input ACL structure before processing
- Used internally by PostgreSQL's privilege checking system