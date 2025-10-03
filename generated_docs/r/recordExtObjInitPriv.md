# recordExtObjInitPriv

## Location
[src/backend/catalog/aclchk.c:4409-4572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4409-L4572)

## Overview
Records the initial privileges (ACLs) for a database object and its sub-objects into pg_init_privs when the object is added to an extension, preserving the original privilege state for potential restoration.

## Definition

```c
void
recordExtObjInitPriv(Oid objoid, Oid classoid)
```
## Detailed Description
This function is part of PostgreSQL's extension system and handles the recording of initial privileges when objects are added to extensions via ALTER EXTENSION ADD. It stores the current ACL state of objects in pg_init_privs so that privileges can be restored when the extension is dropped or when CREATE EXTENSION is run.

The function handles different object types differently:
1. **Relations (tables, views, etc.)**: Records both table-level and column-level ACLs, iterating through all non-dropped columns to capture their individual privileges
2. **Large Objects**: Uses pg_largeobject_metadata to access ACL information (though this is currently dead code as large objects cannot be extension members)
3. **Other Objects**: Uses a generic approach with get_object_attnum_acl() to find the ACL attribute for various object types

The function skips objects that don't have permissions (indexes, partitioned indexes, composite types) and gracefully handles NULL ACLs by not recording entries for them.

## Parameters / Member Variables
- `objoid`: OID of the database object whose privileges should be recorded
- `classoid`: OID of the system catalog class that contains the object (e.g., RelationRelationId, ProcedureRelationId)
## Dependencies
- Functions called/Symbols referenced:
  - [recordExtensionInitPrivWorker](recordExtensionInitPrivWorker.md) (worker function that actually inserts records into pg_init_privs)
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCache2 (system catalog lookups)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md) (extracts attributes from cached tuples)
  - [get_object_attnum_acl](../g/get_object_attnum_acl.md) (gets ACL attribute number for object types)
  - [get_object_catcache_oid](../g/get_object_catcache_oid.md) (gets cache ID for object types)
  - [get_object_class_descr](../g/get_object_class_descr.md) (gets descriptive name for object classes)
  - DatumGetAclP (converts Datum to ACL pointer)
  - Various system catalog access functions
- Called from:
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md) (during ALTER EXTENSION ADD operations)

## Notes and Other Information
- Part of PostgreSQL's extension privilege preservation system
- Records privileges in pg_init_privs for later restoration during CREATE EXTENSION
- Handles complex cases like column-level privileges for relations
- Includes dead code for large objects (cannot currently be extension members)
- Skips objects without permissions (indexes, composite types)
- Uses different access methods based on object type (syscache vs. table scan)
- Essential for maintaining consistent privilege states across extension operations
- The recorded privileges serve as a baseline for privilege restoration when extensions are recreated
- Only records non-NULL ACLs to avoid unnecessary pg_init_privs entries

## Simplified Source

```c
void recordExtObjInitPriv(Oid objoid, Oid classoid) {
    // Handle relations (tables, views, etc.)
    if (classoid == RelationRelationId) {
        HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(objoid));
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for relation %u", objoid);

        Form_pg_class pg_class_tuple = (Form_pg_class) GETSTRUCT(tuple);

        // Skip objects without permissions
        if (pg_class_tuple->relkind == RELKIND_INDEX ||
            pg_class_tuple->relkind == RELKIND_PARTITIONED_INDEX ||
            pg_class_tuple->relkind == RELKIND_COMPOSITE_TYPE) {
            ReleaseSysCache(tuple);
            return;
        }

        // Record column-level ACLs for non-sequences
        if (pg_class_tuple->relkind != RELKIND_SEQUENCE) {
            AttrNumber nattrs = pg_class_tuple->relnatts;

            for (AttrNumber curr_att = 1; curr_att <= nattrs; curr_att++) {
                HeapTuple attTuple = SearchSysCache2(ATTNUM,
                                                    ObjectIdGetDatum(objoid),
                                                    Int16GetDatum(curr_att));

                if (!HeapTupleIsValid(attTuple))
                    continue;

                // Skip dropped columns
                if (((Form_pg_attribute) GETSTRUCT(attTuple))->attisdropped) {
                    ReleaseSysCache(attTuple);
                    continue;
                }

                // Get column ACL and record if not NULL
                Datum attaclDatum = SysCacheGetAttr(ATTNUM, attTuple,
                                                   Anum_pg_attribute_attacl,
                                                   &isNull);

                if (!isNull) {
                    recordExtensionInitPrivWorker(objoid, classoid, curr_att,
                                                 DatumGetAclP(attaclDatum));
                }

                ReleaseSysCache(attTuple);
            }
        }

        // Record table-level ACL
        Datum aclDatum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_relacl,
                                        &isNull);

        if (!isNull) {
            recordExtensionInitPrivWorker(objoid, classoid, 0,
                                         DatumGetAclP(aclDatum));
        }

        ReleaseSysCache(tuple);
    }
    // Handle large objects (dead code - large objects can't be extension members)
    else if (classoid == LargeObjectRelationId) {
        Relation relation = table_open(LargeObjectMetadataRelationId, RowExclusiveLock);

        ScanKeyData entry[1];
        ScanKeyInit(&entry[0], Anum_pg_largeobject_metadata_oid,
                    BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objoid));

        SysScanDesc scan = systable_beginscan(relation,
                                             LargeObjectMetadataOidIndexId,
                                             true, NULL, 1, entry);

        HeapTuple tuple = systable_getnext(scan);
        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "could not find tuple for large object %u", objoid);

        Datum aclDatum = heap_getattr(tuple,
                                     Anum_pg_largeobject_metadata_lomacl,
                                     RelationGetDescr(relation), &isNull);

        if (!isNull) {
            recordExtensionInitPrivWorker(objoid, classoid, 0,
                                         DatumGetAclP(aclDatum));
        }

        systable_endscan(scan);
    }
    // Handle other object types generically
    else if (get_object_attnum_acl(classoid) != InvalidAttrNumber) {
        int cacheid = get_object_catcache_oid(classoid);
        HeapTuple tuple = SearchSysCache1(cacheid, ObjectIdGetDatum(objoid));

        if (!HeapTupleIsValid(tuple))
            elog(ERROR, "cache lookup failed for %s %u",
                 get_object_class_descr(classoid), objoid);

        Datum aclDatum = SysCacheGetAttr(cacheid, tuple,
                                        get_object_attnum_acl(classoid),
                                        &isNull);

        if (!isNull) {
            recordExtensionInitPrivWorker(objoid, classoid, 0,
                                         DatumGetAclP(aclDatum));
        }

        ReleaseSysCache(tuple);
    }
}
```