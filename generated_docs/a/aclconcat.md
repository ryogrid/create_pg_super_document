# aclconcat

## Location
[src/backend/utils/adt/acl.c:477-500](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/acl.c#L477-L500)

## Overview
Concatenates two Access Control Lists (ACLs) into a single new ACL containing all entries from both input ACLs in sequence.

## Definition
```c
Acl *aclconcat(const Acl *left_acl, const Acl *right_acl)
```

## Detailed Description
The `aclconcat` function combines two ACLs by creating a new ACL that contains all entries from the first ACL followed by all entries from the second ACL. The function allocates memory for the combined number of entries and performs two separate memory copy operations to preserve the order of entries. As noted in the source comments, this operation may produce redundant entries if the same grantee appears in both ACLs, so the result should be used carefully and may require subsequent processing to remove duplicates or merge conflicting permissions.

## Parameters / Member Variables
- `left_acl`: Pointer to the first ACL whose entries will appear first in the result
- `right_acl`: Pointer to the second ACL whose entries will be appended after the first ACL's entries

## Dependencies
- Functions called/Symbols referenced:
  - `allocacl` - Allocates memory for the new combined ACL
  - `ACL_NUM` - Macro to get the number of entries in an ACL
  - `ACL_DAT` - Macro to access the data portion of an ACL
  - `memcpy` - Standard library function for memory copying
  - `AclItem` - Structure type representing individual ACL entries
  - `Acl` - ACL structure type definition
- Called from (representative examples):
  - `ExecGrant_Attribute` - During attribute-level permission grants
  - Referenced in `AclResult` type definitions

## Notes and Other Information
- **Warning**: May produce ACLs with redundant or conflicting entries for the same grantee
- The result preserves the exact order of entries from both source ACLs
- No deduplication or merging of permissions is performed during concatenation
- Callers should consider using `aclmerge` if they need to properly combine conflicting permissions
- Memory for the new ACL is allocated in the current memory context
- Both input ACLs remain unmodified (const parameters)
- Commonly used as an intermediate step in more complex ACL manipulation operations