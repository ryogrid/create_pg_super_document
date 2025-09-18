# SetAttrMissing

## Location
src/backend/catalog/heap.c: 2069 - 2129

## Overview
SetAttrMissing sets the missing value for an attribute using a string representation, specifically designed for binary upgrade operations to restore missing value information during PostgreSQL upgrades.

## Definition


## Detailed Description
This function is specifically designed for binary upgrade scenarios to restore missing value information for attributes. It takes a relation OID, attribute name, and string representation of a missing value, then updates the pg_attribute catalog to set atthasmissing to true and stores the parsed missing value in attmissingval. The function acquires an AccessExclusive lock on both the target relation and pg_attribute, validates that the relation is a plain table, looks up the attribute by name, and converts the string value to the appropriate array format using the attribute's type input function. This function ensures that missing value information is properly preserved during PostgreSQL binary upgrades.

## Parameters / Member Variables
- `relid`: OID of the relation containing the attribute
- `attname`: Name of the attribute to set the missing value for
- `value`: String representation of the missing value to be stored

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCacheAttName
  - OidFunctionCall3
  - heap_modify_tuple
  - CatalogTupleUpdate
- Called from (representative examples):
  - binary_upgrade_set_missing_value

## Notes and Other Information
- Designed exclusively for binary upgrade operations, not for general use
- Acquires AccessExclusive lock on the target relation and holds it throughout the operation
- Only operates on plain tables (RELKIND_RELATION), silently returns for other relation types
- Uses attribute name lookup via SearchSysCacheAttName instead of attribute number
- Converts string value to proper array format using F_ARRAY_IN function with type information
- Sets both atthasmissing flag and attmissingval field in pg_attribute
- Part of PostgreSQL's binary upgrade infrastructure for preserving missing value optimizations
- Ensures missing value information is not lost during major version upgrades
- Uses the attribute's type input function to properly parse the string representation
- Maintains data integrity by validating attribute existence before proceeding