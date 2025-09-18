# BuildIndexValueDescription

## Location
src/backend/access/index/genam.c: 176 - 292

## Overview
Constructs a human-readable string representation of index entry contents in the form "(key_name, ...)=(key_value, ...)" for use in error messages.

## Definition


## Detailed Description
BuildIndexValueDescription creates a formatted string describing the contents of an index entry, primarily used for generating informative error messages in unique constraint and exclusion constraint violations. The function takes raw input values (as they would be passed to FormIndexDatum) and produces a readable representation that shows both column names and their corresponding values.

The function implements comprehensive security checks to prevent data leakage by verifying that the user has appropriate SELECT permissions on all key columns of the index. If Row Level Security (RLS) is enabled or if the user lacks sufficient permissions on any column, the function returns NULL rather than exposing potentially sensitive data. For expression-based indexes, it also returns NULL to avoid the complexity of determining which underlying columns are involved.

## Parameters / Member Variables
- : The index relation whose entry is being described
- : Array of Datum values representing the raw input to the index access method
- : Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - IndexRelationGetNumberOfKeyAttributes (get key column count)
  - [check_enable_rls](../c/check_enable_rls.md) (Row Level Security check)
  - [pg_class_aclcheck](../p/pg_class_aclcheck.md) (table-level permission check)
  - [pg_attribute_aclcheck](../p/pg_attribute_aclcheck.md) (column-level permission check)
  - [pg_get_indexdef_columns](../p/pg_get_indexdef_columns.md) (get column names for display)
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md) (get output function for data type)
  - [OidOutputFunctionCall](../O/OidOutputFunctionCall.md) (convert value to string representation)
- Called from (representative examples):
  - [_bt_check_unique](../b/_bt_check_unique.md) (B-tree unique constraint checking)
  - [check_exclusion_or_unique_constraint](../c/check_exclusion_or_unique_constraint.md) (constraint violation handling)
  - [comparetup_index_btree_tiebreak](../c/comparetup_index_btree_tiebreak.md) (tuple sorting operations)

## Notes and Other Information
- Returns NULL if the user lacks SELECT permissions on any key columns to prevent data leakage
- Uses the index opclass input type rather than the stored index type for value formatting
- Only processes key columns of the index, not included columns
- Handles expression-based indexes by returning NULL rather than trying to expose underlying column details
- The function respects Row Level Security policies and will not expose data when RLS is enabled
- Values are formatted using the appropriate output functions for their data types
- NULL values are displayed as the literal string "null" in the output