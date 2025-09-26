# logicalrep_report_missing_attrs

## Location
src/backend/replication/logical/relation.c: 226 - 273

## Overview
Reports an error with the names of missing local relation columns when logical replication encounters attributes that exist in the remote relation but are missing in the local target relation.

## Definition
```c
static void logicalrep_report_missing_attrs(LogicalRepRelation *remoterel, Bitmapset *missingatts)
```

## Detailed Description
This function is a static error reporting utility used in logical replication to provide detailed error messages when the local target relation is missing columns that exist in the remote (source) relation. It constructs a user-friendly error message listing all missing column names and raises an ERROR with appropriate error codes and pluralized messages.

The function iterates through the bitmap of missing attributes, extracts the column names from the remote relation structure, and formats them into a comma-separated list. It uses PostgreSQL's internationalization features to provide properly pluralized error messages depending on whether one or multiple columns are missing.

## Parameters / Member Variables
- `remoterel`: Pointer to LogicalRepRelation structure containing metadata about the remote relation, including column names, namespace, and relation name
- `missingatts`: Bitmapset containing the indexes of attributes that are missing in the local relation but present in the remote relation

## Dependencies
- Functions called/Symbols referenced:
  - bms_is_empty: Checks if the bitmap of missing attributes is empty
  - bms_next_member: Iterates through the bitmap to get the next missing attribute index
  - errmsg_plural: Provides pluralized error messages for internationalization
  - LogicalRepRelation: Structure type representing remote relation metadata
- Called from (representative examples):
  - logicalrep_rel_open: Main function that opens logical replication relations and validates attribute compatibility

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Uses ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE error code to indicate schema mismatch issues
- Supports internationalization through the _() macro and errmsg_plural function
- The error message format distinguishes between single and multiple missing columns for better user experience
- Critical for logical replication integrity as it prevents replication to incompatible target schemas