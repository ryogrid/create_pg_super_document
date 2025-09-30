# removeExtObjInitPriv

## Location
[src/backend/catalog/aclchk.c:4573-4655](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/aclchk.c#L4573-L4655)

## Overview
Removes all initial privilege entries for a database object and its sub-objects from pg_init_privs when the object is dropped from an extension via ALTER EXTENSION DROP.

## Definition

```c
void
removeExtObjInitPriv(Oid objoid, Oid classoid)
```
## Detailed Description
This function serves as the counterpart to recordExtObjInitPriv(), handling the cleanup of pg_init_privs entries when objects are removed from extensions. It systematically removes privilege records for both the main object and any sub-objects (such as columns for relations) by calling recordExtensionInitPrivWorker() with a NULL ACL parameter, which signals deletion.

The function handles relations specially by iterating through all columns (including dropped ones) to ensure complete cleanup of column-level privilege records. Unlike recordExtObjInitPriv(), this function removes records for dropped columns as well, ensuring thorough cleanup when objects are removed from extensions.

The function follows the same object type logic as its recording counterpart, skipping objects that don't have permissions (indexes, partitioned indexes, composite types) but processing relations with potential column-level privileges differently from simple sequences.

## Parameters / Member Variables
- : OID of the database object whose privilege entries should be removed from pg_init_privs
- : OID of the system catalog class containing the object (e.g., RelationRelationId for tables)

## Dependencies
- Functions called/Symbols referenced:
  - [recordExtensionInitPrivWorker](recordExtensionInitPrivWorker.md) (worker function called with NULL ACL to delete entries)
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCache2 (system catalog lookups)
  - HeapTupleIsValid (validates tuple existence)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache references)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md), Int16GetDatum (datum conversion functions)
- Called from:
  - [ExecAlterExtensionContentsRecurse](../E/ExecAlterExtensionContentsRecurse.md) (during ALTER EXTENSION DROP operations)

## Notes and Other Information
- Counterpart function to recordExtObjInitPriv() for extension cleanup
- Part of PostgreSQL's extension privilege management system
- Removes entries from pg_init_privs by calling recordExtensionInitPrivWorker() with NULL ACL
- Handles column-level privilege cleanup for relations, including dropped columns
- Skips objects without permissions (indexes, composite types) similar to the recording function
- Ensures complete cleanup when objects are removed from extensions
- Critical for maintaining pg_init_privs consistency during extension operations
- Unlike recording, removal processes even dropped columns to ensure complete cleanup
- The NULL ACL parameter to recordExtensionInitPrivWorker() triggers deletion logic
- Used during ALTER EXTENSION DROP to clean up privilege tracking entries

## Simplified Source

```c
void removeExtObjInitPriv(Oid objoid, Oid classoid) {
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

        // Remove column-level privilege entries for non-sequences
        if (pg_class_tuple->relkind != RELKIND_SEQUENCE) {
            AttrNumber nattrs = pg_class_tuple->relnatts;

            for (AttrNumber curr_att = 1; curr_att <= nattrs; curr_att++) {
                HeapTuple attTuple = SearchSysCache2(ATTNUM,
                                                    ObjectIdGetDatum(objoid),
                                                    Int16GetDatum(curr_att));

                if (!HeapTupleIsValid(attTuple))
                    continue;

                // Remove privilege entry (including dropped columns)
                recordExtensionInitPrivWorker(objoid, classoid, curr_att, NULL);

                ReleaseSysCache(attTuple);
            }
        }

        ReleaseSysCache(tuple);
    }

    // Remove the top-level object privilege entry
    recordExtensionInitPrivWorker(objoid, classoid, 0, NULL);
}
```