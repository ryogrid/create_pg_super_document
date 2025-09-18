# AlterTableCreateToastTable

## Location
src/backend/catalog/toasting.c: 58 - 63

## Overview
AlterTableCreateToastTable is a function that creates a TOAST table for an existing relation if needed, specifically designed for ALTER TABLE operations.

## Definition


## Detailed Description
This function is a high-level wrapper around CheckAndCreateToastTable that is specifically used during ALTER TABLE operations. It checks if the specified relation needs a TOAST table and creates one if necessary. The function is designed to be called when altering existing tables that might require TOAST storage due to changes in their structure (such as adding large columns). 

The function expects that the caller has already performed necessary permission checks and verified that the relation is indeed a table. It will automatically issue a CommandCounterIncrement if any changes are made to ensure proper catalog visibility.

## Parameters / Member Variables
- : The OID of the relation for which to potentially create a TOAST table
- : Datum containing reloptions for the TOAST table, or (Datum) 0 for default options
- : The lock mode to use when accessing the relation

## Dependencies
- Functions called/Symbols referenced:
  - CheckAndCreateToastTable
- Called from (representative examples):
  - ATRewriteCatalogs (in src/backend/commands/tablecmds.c:5224)

## Notes and Other Information
- This is a specialized variant of TOAST table creation specifically for ALTER TABLE scenarios
- The function passes  for the  parameter and  for the  parameter to CheckAndCreateToastTable
- Callers should ensure proper permissions and relation type validation before calling this function
- The function will increment the command counter if changes are made, affecting catalog visibility