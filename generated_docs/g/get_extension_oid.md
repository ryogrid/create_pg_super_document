# get_extension_oid

## Location
src/backend/commands/extension.c: 145 - 189

## Overview
Looks up the Object Identifier (OID) for a PostgreSQL extension given its name, with optional error handling for missing extensions.

## Definition


## Detailed Description
This function performs a catalog lookup in the pg_extension system catalog to find the OID corresponding to a given extension name. It uses the system catalog scanning interface to search for the extension by name using the ExtensionNameIndexId index for efficient lookups. The function provides flexibility in error handling - it can either throw an error when an extension is not found or return InvalidOid based on the missing_ok parameter.

The function follows PostgreSQL's standard pattern for catalog lookups:
1. Opens the pg_extension system catalog with AccessShareLock
2. Initializes a scan key for the extension name using NAMEEQ operator
3. Performs an indexed scan using ExtensionNameIndexId
4. Extracts the OID from the found tuple or returns InvalidOid if not found
5. Properly cleans up resources by ending the scan and closing the relation

## Parameters / Member Variables
- : The name of the extension to look up (null-terminated C string)
- : Boolean flag controlling error behavior - if false, throws ERROR when extension not found; if true, returns InvalidOid silently

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens pg_extension catalog)
  - ScanKeyInit (initializes search key)
  - systable_beginscan (starts catalog scan)
  - systable_getnext (retrieves next tuple)
  - systable_endscan (ends catalog scan)
  - table_close (closes catalog relation)
  - CStringGetDatum (converts C string to Datum)
  - Form_pg_extension (cast to extension tuple structure)
  - ereport (error reporting)

- Called from (representative examples):
  - CreateExtension (during extension creation)
  - get_required_extension (dependency resolution)
  - AlterExtensionNamespace (namespace changes)
  - get_object_address_unqualified (object addressing)
  - binary_upgrade_create_empty_extension (pg_upgrade support)

## Notes and Other Information
- Assumes at most one matching tuple exists for any given extension name (extensions have unique names)
- Uses AccessShareLock to allow concurrent reads while preventing concurrent schema changes
- Part of PostgreSQL's extension management system introduced to support packaged extensions
- The function is declared in src/include/commands/extension.h and widely used throughout the extension management subsystem
- Returns InvalidOid (0) for non-existent extensions when missing_ok is true, following PostgreSQL conventions