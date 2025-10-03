# RangeCreate

## Location
[src/backend/catalog/pg_range.c:36-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_range.c#L36-L112)

## Overview
Creates an entry in the pg_range catalog table to register metadata for a range type in PostgreSQL's system catalogs.

## Definition

```c
void
RangeCreate(Oid rangeTypeOid, Oid rangeSubType, Oid rangeCollation,
			Oid rangeSubOpclass, RegProcedure rangeCanonical,
			RegProcedure rangeSubDiff, Oid multirangeTypeOid)
```
## Detailed Description
RangeCreate is responsible for creating a new entry in the pg_range catalog table that stores metadata for range types. The function inserts a tuple with range type information including the range type OID, subtype, collation, operator class, canonical function, subdiff function, and associated multirange type. After inserting the catalog entry, it establishes proper dependency relationships between the range type and its constituent objects to ensure referential integrity.

The function operates in several phases:
1. Opens the pg_range catalog table with exclusive row lock
2. Constructs a tuple with the provided range type metadata
3. Inserts the tuple into the catalog
4. Records dependencies between the range type and its referenced objects (subtype, operator class, collation, functions)
5. Records the multirange type's dependency on the range type
6. Closes the catalog table

## Parameters / Member Variables
- `rangeTypeOid`: The OID of the range type being created
- `rangeSubType`: The OID of the element type (subtype) that the range contains
- `rangeCollation`: The OID of the collation to use for the range elements (may be InvalidOid)
- `rangeSubOpclass`: The OID of the operator class for the subtype
- `rangeCanonical`: The OID of the canonical function for normalizing range values (may be InvalidOid)
- `rangeSubDiff`: The OID of the subdiff function for computing differences (may be InvalidOid)
- `multirangeTypeOid`: The OID of the associated multirange type
## Dependencies
- Functions called/Symbols referenced:
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - [new_object_addresses](../n/new_object_addresses.md)
  - ObjectAddressSet
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [record_object_address_dependencies](../r/record_object_address_dependencies.md)
  - [recordDependencyOn](../r/recordDependencyOn.md)
  - [free_object_addresses](../f/free_object_addresses.md)
- Called from (representative examples):
  - [DefineRange](../D/DefineRange.md)

## Notes and Other Information
- The function creates both normal dependencies (DEPENDENCY_NORMAL) for referenced objects like subtypes and operator classes, and an internal dependency (DEPENDENCY_INTERNAL) between the multirange type and range type
- Optional parameters like rangeCollation, rangeCanonical, and rangeSubDiff are only processed if they have valid OIDs
- The dependency system ensures that dropping referenced objects will cascade appropriately to dependent range types
- This function is part of the DDL infrastructure for CREATE TYPE ... AS RANGE commands

## Simplified Source

```c
void
RangeCreate(Oid rangeTypeOid, Oid rangeSubType, Oid rangeCollation,
            Oid rangeSubOpclass, RegProcedure rangeCanonical,
            RegProcedure rangeSubDiff, Oid multirangeTypeOid)
{
    Relation pg_range = table_open(RangeRelationId, RowExclusiveLock);

    // Populate pg_range tuple with all range metadata
    Datum values[Natts_pg_range];
    bool nulls[Natts_pg_range];
    memset(nulls, 0, sizeof(nulls));

    values[Anum_pg_range_rngtypid - 1] = ObjectIdGetDatum(rangeTypeOid);
    values[Anum_pg_range_rngsubtype - 1] = ObjectIdGetDatum(rangeSubType);
    values[Anum_pg_range_rngcollation - 1] = ObjectIdGetDatum(rangeCollation);
    values[Anum_pg_range_rngsubopc - 1] = ObjectIdGetDatum(rangeSubOpclass);
    values[Anum_pg_range_rngcanonical - 1] = ObjectIdGetDatum(rangeCanonical);
    values[Anum_pg_range_rngsubdiff - 1] = ObjectIdGetDatum(rangeSubDiff);
    values[Anum_pg_range_rngmultitypid - 1] = ObjectIdGetDatum(multirangeTypeOid);

    // Insert the catalog entry
    HeapTuple tup = heap_form_tuple(RelationGetDescr(pg_range), values, nulls);
    CatalogTupleInsert(pg_range, tup);
    heap_freetuple(tup);

    // Record dependencies on all referenced objects
    ObjectAddresses *addrs = new_object_addresses();
    ObjectAddress myself = {TypeRelationId, rangeTypeOid, 0};

    // Add dependencies on subtype, operator class, and optional collation/functions
    add_exact_object_address(&(ObjectAddress){TypeRelationId, rangeSubType, 0}, addrs);
    add_exact_object_address(&(ObjectAddress){OperatorClassRelationId, rangeSubOpclass, 0}, addrs);

    if (OidIsValid(rangeCollation))
        add_exact_object_address(&(ObjectAddress){CollationRelationId, rangeCollation, 0}, addrs);
    if (OidIsValid(rangeCanonical))
        add_exact_object_address(&(ObjectAddress){ProcedureRelationId, rangeCanonical, 0}, addrs);
    if (OidIsValid(rangeSubDiff))
        add_exact_object_address(&(ObjectAddress){ProcedureRelationId, rangeSubDiff, 0}, addrs);

    record_object_address_dependencies(&myself, addrs, DEPENDENCY_NORMAL);
    free_object_addresses(addrs);

    // Record multirange type's dependency on this range type
    ObjectAddress multirange = {TypeRelationId, multirangeTypeOid, 0};
    recordDependencyOn(&multirange, &myself, DEPENDENCY_INTERNAL);

    table_close(pg_range, RowExclusiveLock);
}
```