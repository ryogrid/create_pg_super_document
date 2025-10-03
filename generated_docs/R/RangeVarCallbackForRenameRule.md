# RangeVarCallbackForRenameRule

## Location
[src/backend/rewrite/rewriteDefine.c:756-792](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteDefine.c#L756-L792)

## Overview
A callback function that performs permissions and integrity checks before acquiring a relation lock during rule renaming operations.

## Definition

```c
static void
RangeVarCallbackForRenameRule(const RangeVar *rv, Oid relid, Oid oldrelid,
							  void *arg)
```
## Detailed Description
This static callback function is invoked during the relation lock acquisition process for rule rename operations. It validates that the target relation supports rules, ensures the user has appropriate permissions, and prevents modifications to system catalogs when not allowed. The function follows PostgreSQL's standard pattern for RangeVar callbacks, which are used to perform validation checks before acquiring locks on relations. It checks relation kind compatibility (only tables, views, and partitioned tables can have rules), system catalog protection, and ownership requirements.

## Parameters / Member Variables
- `*rv`: Pointer to the RangeVar structure containing the relation name and schema information
- `relid`: Object identifier of the relation being processed
- `oldrelid`: Previous relation OID (used for concurrent drop detection)
- `*arg`: Generic argument pointer (unused in this callback)
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - ereport/errcode/errmsg
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md)
  - [IsSystemClass](../I/IsSystemClass.md)
  - [object_ownercheck](../o/object_ownercheck.md)
  - [aclcheck_error](../a/aclcheck_error.md)
  - [get_relkind_objtype](../g/get_relkind_objtype.md)
  - [get_rel_relkind](../g/get_rel_relkind.md)
  - [GetUserId](../G/GetUserId.md)
  - [ReleaseSysCache](ReleaseSysCache.md)
- Called from (representative examples):
  - [RenameRewriteRule](RenameRewriteRule.md) (via RangeVarGetRelidExtended)

## Notes and Other Information
- Handles concurrent relation drops gracefully by checking tuple validity
- Only allows rules on tables (RELKIND_RELATION), views (RELKIND_VIEW), and partitioned tables (RELKIND_PARTITIONED_TABLE)
- Respects the allowSystemTableMods setting for system catalog protection
- Requires relation ownership for rule rename operations
- Part of PostgreSQL's lock acquisition safety mechanism using RangeVar callbacks
- The function is static, limiting its scope to rewriteDefine.c
- Uses the RELOID system cache for efficient relation metadata lookup

## Simplified Source

```c
static void RangeVarCallbackForRenameRule(const RangeVar *rv, Oid relid, Oid oldrelid, void *arg) {
    HeapTuple tuple;
    Form_pg_class form;

    // Get relation information from system cache
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tuple))
        return; // Relation was concurrently dropped

    form = (Form_pg_class) GETSTRUCT(tuple);

    // Check if relation type supports rules
    if (form->relkind != RELKIND_RELATION &&
        form->relkind != RELKIND_VIEW &&
        form->relkind != RELKIND_PARTITIONED_TABLE)
        ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                       errmsg("relation \"%s\" cannot have rules", rv->relname),
                       errdetail_relkind_not_supported(form->relkind)));

    // Protect system catalogs
    if (!allowSystemTableMods && IsSystemClass(relid, form))
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                       errmsg("permission denied: \"%s\" is a system catalog", rv->relname)));

    // Check ownership requirement
    if (!object_ownercheck(RelationRelationId, relid, GetUserId()))
        aclcheck_error(ACLCHECK_NOT_OWNER, get_relkind_objtype(get_rel_relkind(relid)), rv->relname);

    ReleaseSysCache(tuple);
}
```