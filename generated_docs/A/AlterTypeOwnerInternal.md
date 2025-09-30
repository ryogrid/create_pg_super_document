# AlterTypeOwnerInternal

## Location
[src/backend/commands/typecmds.c:3987-4054](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/typecmds.c#L3987-L4054)

## Overview
Core implementation function that performs the actual pg_type catalog modifications for type ownership changes and recursively handles dependent array and multirange types.

## Definition
```c
void AlterTypeOwnerInternal(Oid typeOid, Oid newOwnerId)
```

## Detailed Description
AlterTypeOwnerInternal is the fundamental function that implements type ownership changes at the catalog level. It directly modifies the pg_type system catalog to update the typowner field and handles ACL (Access Control List) adjustments when necessary. The function implements a recursive strategy to automatically handle dependent types including array types and multirange types.

The function uses heap_modify_tuple to update the type tuple, ensuring atomic updates to both ownership and ACL information. For types with associated array types, it recursively calls itself to maintain consistency. Range types receive special handling where their associated multirange types are also updated recursively.

## Parameters / Member Variables
- `typeOid`: The OID of the type whose ownership is being changed
- `newOwnerId`: The OID of the role that will become the new owner

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [heap_getattr](../h/heap_getattr.md)
  - [aclnewowner](../a/aclnewowner.md)
  - DatumGetAclP
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [get_range_multirange](../g/get_range_multirange.md)
  - [AlterTypeOwnerInternal](AlterTypeOwnerInternal.md) (recursive calls)
  - [table_close](../t/table_close.md)
  - TYPTYPE_RANGE
- Called from (representative examples):
  - [AlterTypeOwner_oid](AlterTypeOwner_oid.md)
  - [ATExecChangeOwner](ATExecChangeOwner.md)
  - [AlterTypeOwnerInternal](AlterTypeOwnerInternal.md) (recursive self-calls)

## Notes and Other Information
- This is a void function that operates directly on the system catalogs
- Uses RowExclusiveLock on TypeRelationId throughout the operation
- Handles ACL updates only when the type has a non-null ACL (typacl field)
- Implements automatic recursive handling of array types via typarray field
- Provides special recursive handling for range types and their associated multirange types
- Updates both typowner and typacl fields atomically using heap_modify_tuple
- Self-recursive design ensures all dependent types maintain ownership consistency
- Used as the lowest-level implementation by both table and type ownership change operations
- Error handling includes validation that multirange types exist for range types

## Simplified Source

```c
void
AlterTypeOwnerInternal(Oid typeOid, Oid newOwnerId)
{
    Relation    rel;
    HeapTuple   tup;
    Form_pg_type typTup;
    Datum       repl_val[Natts_pg_type];
    bool        repl_null[Natts_pg_type];
    bool        repl_repl[Natts_pg_type];
    Acl        *newAcl;
    Datum       aclDatum;
    bool        isNull;

    // Open pg_type relation
    rel = table_open(TypeRelationId, RowExclusiveLock);

    // Get the type tuple
    tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(typeOid));
    if (!HeapTupleIsValid(tup))
        elog(ERROR, "cache lookup failed for type %u", typeOid);

    typTup = (Form_pg_type) GETSTRUCT(tup);

    // Setup modification arrays
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Update owner
    repl_repl[Anum_pg_type_typowner - 1] = true;
    repl_val[Anum_pg_type_typowner - 1] = ObjectIdGetDatum(newOwnerId);

    // Update ACL if present
    aclDatum = heap_getattr(tup, Anum_pg_type_typacl,
                            RelationGetDescr(rel), &isNull);
    if (!isNull)
    {
        newAcl = aclnewowner(DatumGetAclP(aclDatum),
                             typTup->typowner, newOwnerId);
        repl_repl[Anum_pg_type_typacl - 1] = true;
        repl_val[Anum_pg_type_typacl - 1] = PointerGetDatum(newAcl);
    }

    // Update the tuple
    tup = heap_modify_tuple(tup, RelationGetDescr(rel),
                            repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(rel, &tup->t_self, tup);

    // Recursively update array type if it exists
    if (OidIsValid(typTup->typarray))
        AlterTypeOwnerInternal(typTup->typarray, newOwnerId);

    // Recursively update multirange type if this is a range type
    if (typTup->typtype == TYPTYPE_RANGE)
    {
        Oid multirange_typeid = get_range_multirange(typeOid);
        if (!OidIsValid(multirange_typeid))
            ereport(ERROR,
                    (errcode(ERRCODE_UNDEFINED_OBJECT),
                     errmsg("could not find multirange type for data type %s",
                            format_type_be(typeOid))));
        AlterTypeOwnerInternal(multirange_typeid, newOwnerId);
    }

    table_close(rel, RowExclusiveLock);
}
```