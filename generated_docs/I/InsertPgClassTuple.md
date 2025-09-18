# InsertPgClassTuple

## Location
src/backend/catalog/heap.c: 896 - 968

## Overview
Constructs and inserts a new tuple into the pg_class system catalog to register a relation's metadata and properties.

## Definition


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
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_freetuple
  - Various Datum conversion functions (ObjectIdGetDatum, NameGetDatum, Int32GetDatum, etc.)
- Called from (representative examples):
  - AddNewRelationTuple
  - index_create

## Notes and Other Information
- The caller must have already opened and locked the pg_class relation before calling this function
- The relpartbound field is always set to NULL initially and updated separately if needed for partitioned tables
- Variable-width fields (relacl, reloptions) are handled specially since they're not present in cached relation descriptors
- The function assumes all fixed-width relation metadata is available in the new_rel_desc->rd_rel structure
- Memory management is handled automatically - the function allocates and frees the tuple as needed