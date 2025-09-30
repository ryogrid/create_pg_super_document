# RangeVarCallbackForLockTable

## Location
[src/backend/commands/lockcmds.c:71-116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/lockcmds.c#L71-L116)

## Overview
A callback function that validates permissions and relation types before acquiring a table lock, ensuring only appropriate relations can be locked with proper authorization.

## Definition

```c
static void
RangeVarCallbackForLockTable(const RangeVar *rv, Oid relid, Oid oldrelid,
							 void *arg)
```
## Detailed Description
RangeVarCallbackForLockTable serves as a security and validation callback invoked during the relation resolution process in LOCK TABLE commands. It performs several critical checks: validates that the relation type is lockable (tables, partitioned tables, or views only), checks user permissions for the requested lock mode, and tracks access to temporary relations for transaction flag management. This callback ensures that lock operations are both authorized and semantically valid before proceeding with actual lock acquisition.

## Parameters / Member Variables
- : Pointer to RangeVar structure representing the relation being locked
- : OID of the resolved relation, or InvalidOid if relation doesn't exist
- : Previous OID if relation was concurrently modified (used for detecting concurrent DDL)
- : Void pointer containing the requested LOCKMODE cast as argument

## Dependencies
- Functions called/Symbols referenced:
  - [RangeVar](RangeVar.md) (structure type)
  - [AclResult](../A/AclResult.md) (enum type)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - RELKIND_RELATION
  - RELKIND_PARTITIONED_TABLE
  - RELKIND_VIEW
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [get_rel_persistence](../g/get_rel_persistence.md)
  - RELPERSISTENCE_TEMP
  - XACT_FLAGS_ACCESSEDTEMPNAMESPACE
  - [LockTableAclCheck](../L/LockTableAclCheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
- Called from (representative examples):
  - [LockTableCommand](../L/LockTableCommand.md) (via RangeVarGetRelidExtended)

## Notes and Other Information
- This is a static function, only accessible within the lockcmds.c module
- The function gracefully handles concurrent relation drops by checking for valid relation kinds
- Only plain tables, partitioned tables, and views are allowed to be locked; other relation types generate errors
- Temporary relation access is tracked for proper transaction state management
- Permission checking is delegated to LockTableAclCheck for the specific lock mode requested
- The callback pattern allows for consistent validation across different relation resolution contexts

## Simplified Source
```c
static void RangeVarCallbackForLockTable(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg) {
    LOCKMODE lockmode = *(LOCKMODE *) arg;
    char relkind;
    char relpersistence;
    AclResult aclresult;

    // Skip validation if relation doesn't exist
    if (!OidIsValid(relid))
        return;

    // Get relation kind, handle concurrent drops
    relkind = get_rel_relkind(relid);
    if (!relkind)
        return;  // Relation was dropped concurrently

    // Only allow locking of tables, partitioned tables, and views
    if (relkind != RELKIND_RELATION &&
        relkind != RELKIND_PARTITIONED_TABLE &&
        relkind != RELKIND_VIEW)
        ereport(ERROR, "cannot lock relation - wrong object type");

    // Track temporary relation access for transaction flags
    relpersistence = get_rel_persistence(relid);
    if (relpersistence == RELPERSISTENCE_TEMP)
        MyXactFlags |= XACT_FLAGS_ACCESSEDTEMPNAMESPACE;

    // Check user permissions for the requested lock mode
    aclresult = LockTableAclCheck(relid, lockmode, GetUserId());
    if (aclresult != ACLCHECK_OK)
        aclcheck_error(aclresult, get_relkind_objtype(relkind), rv->relname);
}
```