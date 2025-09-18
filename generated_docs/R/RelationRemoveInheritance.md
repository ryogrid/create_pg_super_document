# RelationRemoveInheritance

## Location
src/backend/catalog/heap.c: 1526 - 1558

## Overview
Removes inheritance relationships for a relation by deleting all pg_inherits catalog entries where the relation appears as a child.

## Definition


## Detailed Description
This function is responsible for cleaning up inheritance metadata when a relation is being dropped. It operates on the pg_inherits system catalog, which stores parent-child relationships between tables in PostgreSQL's table inheritance hierarchy.

The function performs a systematic scan of pg_inherits to find all rows where the specified relation ID appears as the child relation (inhrelid column). For each matching inheritance entry found, it deletes the tuple from the catalog, effectively severing the inheritance relationship between the child and its parent(s).

Importantly, this function has evolved from earlier PostgreSQL versions. Previously, it would check for child relations and abort deletion if any were found. However, the current implementation relies on PostgreSQL's dependency mechanism to handle child relation checking and deletion. By the time this function is called, the dependency system has already ensured that no child relations exist, so this function only needs to clean up the inheritance metadata.

The function uses PostgreSQL's system catalog scanning infrastructure to efficiently locate and delete the relevant inheritance entries using the InheritsRelidSeqnoIndexId index for fast lookups.

## Parameters / Member Variables
- : The OID of the relation whose inheritance relationships should be removed from pg_inherits

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens pg_inherits catalog for modification)
  - ScanKeyInit (initializes scan key for relid lookup)
  - systable_beginscan (begins indexed scan on pg_inherits)
  - systable_getnext (retrieves next matching tuple)
  - CatalogTupleDelete (deletes inheritance tuple from catalog)
  - systable_endscan (ends the system catalog scan)
  - table_close (closes pg_inherits catalog relation)
- Called from (representative examples):
  - heap_drop_with_catalog (during relation deletion process)

## Notes and Other Information
- This is a static function within heap.c, meaning it's only used internally within the heap management module
- The function assumes that dependency checking has already been performed and no child relations exist
- Uses RowExclusiveLock on pg_inherits to ensure exclusive access during the deletion operation
- The scan uses the InheritsRelidSeqnoIndexId index for efficient lookup of inheritance entries by child relation ID
- This function only handles the child side of inheritance relationships; parent relationships are handled through the dependency system
- The function is part of the relation deletion workflow and is called during the cleanup phase of dropping tables