# RTEPermissionInfo

## Location
[src/include/nodes/parsenodes.h:1286-1297](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L1286-L1297)

## Overview
RTEPermissionInfo contains per-relation information for permission checking, storing access control requirements and column-level permissions needed for query execution.

## Definition

```c
typedef struct RTEPermissionInfo
{
	NodeTag		type;

	Oid			relid;			/* relation OID */
	bool		inh;			/* separately check inheritance children? */
	AclMode		requiredPerms;	/* bitmask of required access permissions */
	Oid			checkAsUser;	/* if valid, check access as this role */
	Bitmapset  *selectedCols;	/* columns needing SELECT permission */
	Bitmapset  *insertedCols;	/* columns needing INSERT permission */
	Bitmapset  *updatedCols;	/* columns needing UPDATE permission */
} RTEPermissionInfo;
```
## Detailed Description
RTEPermissionInfo is added to Query nodes by the parser for each relation that requires permission checking. It specifies run-time access permissions that must be verified at query startup. The structure supports both table-wide and column-level permissions, allowing fine-grained access control.

The permission checking system requires users to have ALL permissions specified in requiredPerms (never 0). The checkAsUser field enables rules to act as setuid gateways by checking permissions using a different user's privileges rather than the current effective user.

For SELECT/INSERT/UPDATE operations, if table-wide permissions are insufficient, column-specific permissions in the respective bitmapsets are checked. The bitmapsets store column numbers adjusted by subtracting FirstLowInvalidHeapAttributeNumber, and whole-row references are represented by setting the InvalidAttrNumber bit.

## Parameters / Member Variables
- `type`: NodeTag identifying this as an RTEPermissionInfo node
- `relid`: OID of the relation requiring permission checks
- `inh`: Whether inheritance children should be checked separately (for extensions)
- `requiredPerms`: Bitmask of required access permissions (ACL_SELECT, ACL_INSERT, etc.)
- `checkAsUser`: OID of user whose privileges to use for checking (0 for current user)
- `*selectedCols`: Bitmapset of columns requiring SELECT permission
- `*insertedCols`: Bitmapset of columns requiring INSERT permission
- `*updatedCols`: Bitmapset of columns requiring UPDATE permission
## Dependencies
- Functions called/Symbols referenced:
  - NodeTag
  - Oid
  - AclMode
  - [Bitmapset](../B/Bitmapset.md)
- Called from (representative examples):
  - [addRTEPermissionInfo](../a/addRTEPermissionInfo.md)
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [ExecCheckPermissions](../E/ExecCheckPermissions.md)
  - [ExecCheckOneRelPerms](../E/ExecCheckOneRelPerms.md)
  - [transformInsertStmt](../t/transformInsertStmt.md)
  - [transformUpdateTargetList](../t/transformUpdateTargetList.md)
  - [markRTEForSelectPriv](../m/markRTEForSelectPriv.md)
  - [rewriteTargetView](../r/rewriteTargetView.md)

## Notes and Other Information
- Only relations directly mentioned in queries have RTEPermissionInfos in the Query node
- Extensions can use the 'inh' flag to determine whether to check inheritance children
- Column numbers in bitmapsets are adjusted to handle negative attribute numbers
- updatedCols is used beyond permissions checking (trigger firing, FDW column shipping)
- The structure is editorialized by the rewriter after rule expansion
- Whole-row Var references are represented using InvalidAttrNumber bit
- Used extensively in executor permission checking and optimizer planning phases