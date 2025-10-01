# recordExtensionInitPrivWorker

## Location
[src/backend/catalog/aclchk.c:4685-4812](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4685-L4812)

## Overview
The worker function that performs the actual recording of initial ACL for extension objects in the pg_init_privs system catalog table.

## Definition
static void recordExtensionInitPrivWorker(Oid objoid, Oid classoid, int objsubid, Acl *new_acl)

## Detailed Description
This function performs the core work of recording or updating initial privileges for extension objects in the pg_init_privs catalog. It handles wholesale replacement of ACL entries and maintains dependency tracking through pg_shdepend. The function can insert new entries, update existing ones, or delete entries when passed a NULL ACL. Unlike recordExtensionInitPriv, this worker function does not check the creating_extension flag, making it suitable for use during ALTER EXTENSION operations.

The function uses a systematic approach: it first searches for existing entries, then either updates them or creates new ones as needed. It also maintains role membership dependencies by calling updateInitAclDependencies to ensure proper tracking of roles referenced in ACLs.

## Parameters / Member Variables
- objoid: The OID of the object whose initial privileges are being recorded
- classoid: The OID of the system catalog table that defines the object type
- objsubid: Sub-object identifier (0 for objects without sub-components, positive for table columns)
- new_acl: The complete new ACL to store; NULL indicates the entry should be removed

## Dependencies
- Functions called/Symbols referenced:
  - [aclmembers](../a/aclmembers.md)
  - [table_open](../t/table_open.md)/table_close
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan/systable_getnext
  - [heap_getattr](../h/heap_getattr.md)
  - DatumGetAclP
  - [updateInitAclDependencies](../u/updateInitAclDependencies.md)
  - ACL_NUM
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)/CatalogTupleDelete/CatalogTupleInsert
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
  - INITPRIVS_EXTENSION
  - [CharGetDatum](../C/CharGetDatum.md)
- Called from (representative examples):
  - InternalDefaultACL
  - [recordExtObjInitPriv](recordExtObjInitPriv.md)
  - [removeExtObjInitPriv](removeExtObjInitPriv.md)
  - [recordExtensionInitPriv](recordExtensionInitPriv.md)

## Notes and Other Information
- Performs wholesale replacement of ACL entries, requiring complete ACL specification
- Maintains role dependency tracking through updateInitAclDependencies
- Uses RowExclusiveLock on pg_init_privs to ensure consistency during updates
- Handles both creation and deletion of initial privilege entries
- Specifically designed for INITPRIVS_EXTENSION privilege type
- Includes CommandCounterIncrement to handle multiple object processing
- Does not check creating_extension flag unlike its wrapper function

## Simplified Source

```c
static void recordExtensionInitPrivWorker(Oid objoid, Oid classoid, int objsubid, Acl *new_acl) {
    Relation relation;
    ScanKeyData key[3];
    SysScanDesc scan;
    HeapTuple tuple, oldtuple;
    int noldmembers, nnewmembers;
    Oid *oldmembers, *newmembers;

    // Extract role membership from new ACL
    nnewmembers = aclmembers(new_acl, &newmembers);

    // Open pg_init_privs catalog for search and modification
    relation = table_open(InitPrivsRelationId, RowExclusiveLock);

    // Set up scan keys to find existing entry
    ScanKeyInit(&key[0], Anum_pg_init_privs_objoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(objoid));
    ScanKeyInit(&key[1], Anum_pg_init_privs_classoid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(classoid));
    ScanKeyInit(&key[2], Anum_pg_init_privs_objsubid, BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(objsubid));

    scan = systable_beginscan(relation, InitPrivsObjIndexId, true, NULL, 3, key);
    oldtuple = systable_getnext(scan);

    if (HeapTupleIsValid(oldtuple)) {
        // Found existing entry - update or delete it
        Datum oldAclDatum;
        bool isNull;
        Acl *old_acl;

        // Get old ACL for dependency tracking
        oldAclDatum = heap_getattr(oldtuple, Anum_pg_init_privs_initprivs, RelationGetDescr(relation), &isNull);
        Assert(!isNull);
        old_acl = DatumGetAclP(oldAclDatum);
        noldmembers = aclmembers(old_acl, &oldmembers);

        // Update role dependencies
        updateInitAclDependencies(classoid, objoid, objsubid, noldmembers, oldmembers, nnewmembers, newmembers);

        if (new_acl && ACL_NUM(new_acl) != 0) {
            // Update existing entry with new ACL
            Datum values[Natts_pg_init_privs] = {0};
            bool nulls[Natts_pg_init_privs] = {0};
            bool replace[Natts_pg_init_privs] = {0};

            values[Anum_pg_init_privs_initprivs - 1] = PointerGetDatum(new_acl);
            replace[Anum_pg_init_privs_initprivs - 1] = true;

            oldtuple = heap_modify_tuple(oldtuple, RelationGetDescr(relation), values, nulls, replace);
            CatalogTupleUpdate(relation, &oldtuple->t_self, oldtuple);
        } else {
            // New ACL is NULL/empty - delete the entry
            CatalogTupleDelete(relation, &oldtuple->t_self);
        }
    } else if (new_acl && ACL_NUM(new_acl) != 0) {
        // No existing entry found - create new one
        Datum values[Natts_pg_init_privs] = {0};
        bool nulls[Natts_pg_init_privs] = {0};

        values[Anum_pg_init_privs_objoid - 1] = ObjectIdGetDatum(objoid);
        values[Anum_pg_init_privs_classoid - 1] = ObjectIdGetDatum(classoid);
        values[Anum_pg_init_privs_objsubid - 1] = Int32GetDatum(objsubid);
        values[Anum_pg_init_privs_privtype - 1] = CharGetDatum(INITPRIVS_EXTENSION);
        values[Anum_pg_init_privs_initprivs - 1] = PointerGetDatum(new_acl);

        tuple = heap_form_tuple(RelationGetDescr(relation), values, nulls);
        CatalogTupleInsert(relation, tuple);

        // Update dependencies for new entry
        noldmembers = 0;
        oldmembers = NULL;
        updateInitAclDependencies(classoid, objoid, objsubid, noldmembers, oldmembers, nnewmembers, newmembers);
    }

    systable_endscan(scan);
    CommandCounterIncrement();  // Prevent errors when processing multiple objects
    table_close(relation, RowExclusiveLock);
}
```