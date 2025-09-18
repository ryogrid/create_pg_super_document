# BootstrapToastTable

## Location
src/backend/catalog/toasting.c: 98 - 126

## Overview
BootstrapToastTable is a specialized function for creating TOAST tables during the database bootstrap process, where TOAST table and index OIDs must be pre-specified.

## Definition


## Detailed Description
This function is specifically designed for use during PostgreSQL's bootstrap process when the system catalogs are being initialized. Unlike the other TOAST table creation functions that automatically assign OIDs, this function requires pre-specified OIDs for both the TOAST table and its index. This is necessary during bootstrap because the system needs to create TOAST tables with predetermined OIDs that match the catalog definitions.

The function performs validation to ensure the target relation is either a regular table or materialized view, as these are the only relation types that can have TOAST tables. It also includes error handling to detect cases where a TOAST table is not actually needed for the relation, which would indicate a problem in the bootstrap data.

The bootstrap context means this function operates in a controlled environment where catalog consistency is critical, hence the requirement for pre-specified OIDs and the stricter error handling compared to the runtime TOAST table creation functions.

## Parameters / Member Variables
- : The name of the relation for which to create a TOAST table
- : The pre-assigned OID for the TOAST table to be created
- : The pre-assigned OID for the TOAST table's index

## Dependencies
- Functions called/Symbols referenced:
  - table_openrv
  - [makeRangeVar](../m/makeRangeVar.md)
  - AccessExclusiveLock
  - [create_toast_table](../c/create_toast_table.md)
  - RELKIND_RELATION
  - RELKIND_MATVIEW
- Called from (representative examples):
  - Used during bootstrap process (referenced in src/include/catalog/toasting.h:27)

## Notes and Other Information
- This is the only TOAST table creation function that requires pre-specified OIDs for both table and index
- Used exclusively during database bootstrap when system catalogs are being initialized
- Includes strict validation that the relation is a table or materialized view
- Will throw an ERROR if the relation doesn't actually need a TOAST table, indicating bootstrap data inconsistency
- Uses AccessExclusiveLock consistently, appropriate for the bootstrap environment
- The function name uses the relation name (string) rather than OID, typical for bootstrap operations