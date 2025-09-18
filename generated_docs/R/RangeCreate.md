# RangeCreate

## Location
src/backend/catalog/pg_range.c: 36 - 112

## Overview
Creates an entry in the pg_range catalog table to register metadata for a range type in PostgreSQL's system catalogs.

## Definition


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
- : The OID of the range type being created
- : The OID of the element type (subtype) that the range contains
- : The OID of the collation to use for the range elements (may be InvalidOid)
- : The OID of the operator class for the subtype
- : The OID of the canonical function for normalizing range values (may be InvalidOid)
- : The OID of the subdiff function for computing differences (may be InvalidOid)
- : The OID of the associated multirange type

## Dependencies
- Functions called/Symbols referenced:
  - heap_form_tuple
  - CatalogTupleInsert
  - heap_freetuple
  - new_object_addresses
  - ObjectAddressSet
  - add_exact_object_address
  - record_object_address_dependencies
  - recordDependencyOn
  - free_object_addresses
- Called from (representative examples):
  - DefineRange

## Notes and Other Information
- The function creates both normal dependencies (DEPENDENCY_NORMAL) for referenced objects like subtypes and operator classes, and an internal dependency (DEPENDENCY_INTERNAL) between the multirange type and range type
- Optional parameters like rangeCollation, rangeCanonical, and rangeSubDiff are only processed if they have valid OIDs
- The dependency system ensures that dropping referenced objects will cascade appropriately to dependent range types
- This function is part of the DDL infrastructure for CREATE TYPE ... AS RANGE commands