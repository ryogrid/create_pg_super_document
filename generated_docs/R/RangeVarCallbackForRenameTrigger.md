# RangeVarCallbackForRenameTrigger

## Location
[src/backend/commands/trigger.c:1416-1462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/trigger.c#L1416-L1462)

## Overview
A callback function that performs permissions and integrity checks before acquiring a relation lock during trigger rename operations.

## Definition

```c
static void
RangeVarCallbackForRenameTrigger(const RangeVar *rv, Oid relid, Oid oldrelid,
								 void *arg)
```
## Detailed Description
This function serves as a validation callback used during trigger rename operations to ensure that the target relation is appropriate for trigger operations and that the user has sufficient privileges. It performs three main checks: relation kind validation (ensuring only tables, views, foreign tables, and partitioned tables can have triggers), ownership verification (ensuring the user owns the relation), and system catalog protection (preventing modifications to system catalogs unless explicitly allowed).

The function is designed to be called by the RangeVar resolution mechanism before acquiring locks on the target relation, allowing for early detection of permission or compatibility issues.

## Parameters / Member Variables
- `*rv`: Pointer to RangeVar structure containing the relation name and schema information
- `relid`: Object identifier of the resolved relation
- `oldrelid`: Previous relation OID (used for detecting concurrent changes)
- `*arg`: Generic argument pointer (unused in this implementation)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [ReleaseSysCache](ReleaseSysCache.md)
- Called from (representative examples):
  - [renametrig](../r/renametrig.md)

## Notes and Other Information
- Only allows trigger operations on relations of kind RELKIND_RELATION, RELKIND_VIEW, RELKIND_FOREIGN_TABLE, and RELKIND_PARTITIONED_TABLE
- Enforces ownership requirements through object_ownercheck() before allowing trigger modifications
- Respects allowSystemTableMods setting to control system catalog modifications
- Uses system cache lookups for efficient relation metadata access
- Handles concurrent relation drops gracefully by checking tuple validity

## Simplified Source

```c
static void RangeVarCallbackForRenameTrigger(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg)
{
    // Get relation metadata from system catalog
    HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return; // Handle concurrent drops

    Form_pg_class form = (Form_pg_class) GETSTRUCT(tuple);

    // Validate relation kind - only certain types can have triggers
    if (form->relkind != RELKIND_RELATION && form->relkind != RELKIND_VIEW &&
        form->relkind != RELKIND_FOREIGN_TABLE && form->relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("relation \"%s\" cannot have triggers", rv->relname),
                 errdetail_relkind_not_supported(form->relkind)));

    // Check ownership - user must own the relation
    if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);

    // Prevent system catalog modifications unless explicitly allowed
    if (!allowSystemTableMods && IsSystemClass(relid, form))
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 errmsg("permission denied: \"%s\" is a system catalog", rv->relname)));

    ReleaseSysCache(tuple);
}
```