# needs_toast_table

## Location
src/backend/catalog/toasting.c: 401 - 427

## Overview
Determines whether a given relation requires a TOAST table by applying various exclusion criteria and delegating the final decision to the access method.

## Definition
static bool needs_toast_table(Relation rel)

## Detailed Description
This function implements the decision logic for whether a table should have an associated TOAST table created. It serves as a filter that applies several PostgreSQL-specific rules before delegating to the access method's own evaluation logic. The function performs a series of checks to exclude certain types of relations that should never have TOAST tables, ensuring system consistency and preventing problematic configurations.

The function first checks for partitioned tables, which don't need TOAST tables since their partitions handle toasting individually. It then applies restrictions for shared relations (which cannot be toasted after initdb due to cross-database visibility issues) and catalog relations (whose TOAST table creation is explicitly controlled during system initialization). After these exclusions, it defers to the table's access method to make the final determination based on the table's structure and storage requirements.

## Parameters / Member Variables
- `rel`: The relation (table) to evaluate for TOAST table necessity, must be properly opened

## Dependencies
- Functions called/Symbols referenced:
  - IsBootstrapProcessingMode
  - [IsCatalogRelation](../I/IsCatalogRelation.md)  
  - [table_relation_needs_toast_table](../t/table_relation_needs_toast_table.md)
- Called from (representative examples):
  - [create_toast_table](../c/create_toast_table.md)

## Notes and Other Information
- Returns true if the relation should have a TOAST table, false otherwise
- Partitioned tables never need TOAST tables as partitions handle their own toasting
- Shared relations cannot be toasted after initdb due to pg_class synchronization issues across databases
- Catalog table toasting is explicitly controlled via catalog/pg_*.h definitions during bootstrap
- The final decision is delegated to the access method via table_relation_needs_toast_table()
- This function acts as a policy layer above the access method's technical evaluation