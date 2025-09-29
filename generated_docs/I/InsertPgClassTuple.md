# InsertPgClassTuple

## Location
[src/backend/catalog/heap.c:896-968](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/heap.c#L896-L968)

## Overview
Constructs and inserts a new tuple into the pg_class system catalog to register a relation's metadata and properties.

## Definition

```c
void
InsertPgClassTuple(Relation pg_class_desc,
				   Relation new_rel_desc,
				   Oid new_rel_oid,
				   Datum relacl,
				   Datum reloptions)
```
## Detailed Description
InsertPgClassTuple is a low-level catalog management function that creates and inserts a pg_class tuple for a newly created relation. It extracts relation metadata from the relation descriptor's rd_rel field and constructs a complete pg_class tuple with all necessary fields properly formatted.

The function handles the conversion of various data types to their appropriate Datum representations and manages nullable fields correctly. It copies most field values directly from the relation descriptor but allows for external specification of variable-width fields like relacl (access control list) and reloptions (relation options) which are not stored in the cached relation descriptor.

The function uses heap_form_tuple to construct the tuple from arrays of values and null indicators, then inserts it into pg_class using CatalogTupleInsert. It properly manages memory by freeing the constructed tuple after insertion.

## Parameters / Member Variables
- : Already opened and locked relation handle for the pg_class catalog
- : Relation descriptor for the new relation being registered; provides most field values via rd_rel
- : OID to assign to the new relation entry in pg_class
- : Datum containing the access control list for the relation; pass (Datum) 0 to set to NULL
- : Datum containing relation options; pass (Datum) 0 to set to NULL

## Dependencies
- Functions called/Symbols referenced:
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [CatalogTupleInsert](../C/CatalogTupleInsert.md)
  - [heap_freetuple](../h/heap_freetuple.md)
  - Various Datum conversion functions (ObjectIdGetDatum, NameGetDatum, Int32GetDatum, etc.)
- Called from (representative examples):
  - [AddNewRelationTuple](../A/AddNewRelationTuple.md)
  - [index_create](../i/index_create.md)

## Notes and Other Information
- The caller must have already opened and locked the pg_class relation before calling this function
- The relpartbound field is always set to NULL initially and updated separately if needed for partitioned tables
- [Variable](../V/Variable.md)-width fields (relacl, reloptions) are handled specially since they're not present in cached relation descriptors
- The function assumes all fixed-width relation metadata is available in the new_rel_desc->rd_rel structure
- Memory management is handled automatically - the function allocates and frees the tuple as needed

## Simplified Source

```c
void
InsertPgClassTuple(Relation pg_class_desc, Relation new_rel_desc,
                   Oid new_rel_oid, Datum relacl, Datum reloptions)
{
    Form_pg_class rd_rel = new_rel_desc->rd_rel;
    Datum values[Natts_pg_class];
    bool nulls[Natts_pg_class];

    // Initialize arrays
    memset(values, 0, sizeof(values));
    memset(nulls, false, sizeof(nulls));

    // Copy standard fields from relation descriptor
    values[Anum_pg_class_oid - 1] = ObjectIdGetDatum(new_rel_oid);
    values[Anum_pg_class_relname - 1] = NameGetDatum(&rd_rel->relname);
    values[Anum_pg_class_relnamespace - 1] = ObjectIdGetDatum(rd_rel->relnamespace);
    values[Anum_pg_class_reltype - 1] = ObjectIdGetDatum(rd_rel->reltype);
    values[Anum_pg_class_relowner - 1] = ObjectIdGetDatum(rd_rel->relowner);
    values[Anum_pg_class_relam - 1] = ObjectIdGetDatum(rd_rel->relam);
    values[Anum_pg_class_relkind - 1] = CharGetDatum(rd_rel->relkind);
    values[Anum_pg_class_relnatts - 1] = Int16GetDatum(rd_rel->relnatts);
    // ... (additional field assignments)

    // Handle variable-width fields
    if (relacl != (Datum) 0)
        values[Anum_pg_class_relacl - 1] = relacl;
    else
        nulls[Anum_pg_class_relacl - 1] = true;

    if (reloptions != (Datum) 0)
        values[Anum_pg_class_reloptions - 1] = reloptions;
    else
        nulls[Anum_pg_class_reloptions - 1] = true;

    // Set relpartbound to NULL initially
    nulls[Anum_pg_class_relpartbound - 1] = true;

    // Create and insert tuple
    HeapTuple tup = heap_form_tuple(RelationGetDescr(pg_class_desc), values, nulls);
    CatalogTupleInsert(pg_class_desc, tup);
    heap_freetuple(tup);
}
```