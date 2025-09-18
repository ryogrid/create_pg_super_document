# AddNewRelationTuple

## Location
src/backend/catalog/heap.c: 969 - 1026

## Overview
Registers a new relation in the system catalogs by updating the relation descriptor and adding a tuple to pg_class.

## Definition


## Detailed Description
AddNewRelationTuple is a high-level catalog management function that completes the registration of a new relation in PostgreSQL's system catalogs. It serves as an intermediary between relation creation logic and the low-level InsertPgClassTuple function, handling the preparation and updating of relation metadata before catalog insertion.

The function performs two main operations: first, it updates the relation descriptor's rd_rel structure with appropriate initial values and metadata; second, it calls InsertPgClassTuple to actually insert the tuple into pg_class. The function handles special cases like sequences which have known initial sizes, and ensures that new relations start with appropriate statistics (empty pages, no tuples initially).

The function also properly initializes the relation's tuple descriptor type information, setting tdtypeid and tdtypmod to appropriate values. It ensures relispartition is initially false, as partition status is set through subsequent updates when needed.

## Parameters / Member Variables
- : Already opened and locked relation handle for the pg_class catalog
- : Relation descriptor for the new relation being created
- : OID assigned to the new relation
- : OID of the composite type associated with this relation (may be InvalidOid)
- : OID of the type this relation is "of" (for typed tables); usually InvalidOid
- : OID of the user who owns this relation
- : Character indicating the kind of relation (table, index, sequence, etc.)
- : Transaction ID for frozen tuple visibility
- : Minimum MultiXact ID for the relation
- : Access control list for the relation (may be NULL)
- : Relation options (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [InsertPgClassTuple](../I/InsertPgClassTuple.md)
- Called from (representative examples):
  - [heap_create_with_catalog](../h/heap_create_with_catalog.md)

## Notes and Other Information
- This function is static and primarily used during relation creation as part of heap_create_with_catalog
- New relations are initialized with empty statistics (0 pages, -1 tuples) except for sequences which have known sizes
- The relispartition field is always initialized to false and updated later if the relation becomes a partition
- The function ensures proper type information is set in the tuple descriptor even when reltype is zero (using RECORDOID as fallback)
- Transaction IDs for visibility are properly initialized to maintain MVCC consistency