# FormExtraData_pg_attribute

## Location
[src/include/catalog/pg_attribute.h:219-223](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/catalog/pg_attribute.h#L219-L223)

## Overview
FormExtraData_pg_attribute is a supplementary structure that contains additional fields for pg_attribute that are excluded from the main FormData_pg_attribute structure due to CATALOG_VARLEN constraints.

## Definition


## Detailed Description
This structure is designed to work in conjunction with FormData_pg_attribute to provide a complete representation of column attribute information in PostgreSQL's DDL (Data Definition Language) operations. The structure contains fields that cannot be included in the main FormData_pg_attribute structure because they are variable-length fields that are excluded by the CATALOG_VARLEN mechanism.

The structure serves as a bridge between the catalog system's constraints and the need for DDL code to access all attribute information. By combining FormData_pg_attribute (often accessed via tuple descriptors) with FormExtraData_pg_attribute, DDL operations can work with complete attribute metadata.

## Parameters / Member Variables
- : A NullableDatum containing the statistics target for the attribute, which controls the level of detail collected by ANALYZE
- : A NullableDatum containing attribute-specific options stored as a text array

## Dependencies
- Functions called/Symbols referenced:
  - [NullableDatum](../N/NullableDatum.md)
- Called from (representative examples):
  - [InsertPgAttributeTuples](../I/InsertPgAttributeTuples.md) (src/backend/catalog/heap.c:706, 728)
  - [AppendAttributeTuples](../A/AppendAttributeTuples.md) (src/backend/catalog/index.c:515, 519)

## Notes and Other Information
- This structure is specifically designed for DDL code usage
- The separation from FormData_pg_attribute is due to PostgreSQL's catalog system constraints regarding variable-length fields
- The NullableDatum type allows these fields to handle NULL values appropriately
- Additional fields can be added to this structure as needed for future DDL operations
- The structure is defined in src/include/catalog/pg_attribute.h at lines 219-223