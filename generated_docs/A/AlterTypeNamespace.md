# AlterTypeNamespace

## Location
src/backend/commands/typecmds.c: 4055 - 4103

## Overview
Executes ALTER TYPE SET SCHEMA command to move a PostgreSQL type from one schema (namespace) to another, with proper validation and dependency tracking.

## Definition


## Detailed Description
AlterTypeNamespace is the main entry point for handling ALTER TYPE SET SCHEMA SQL commands. It validates the type name, ensures proper object type constraints (particularly for domains), resolves the target schema, and delegates the actual namespace change operation to AlterTypeNamespace_oid. The function performs comprehensive error checking to prevent invalid operations like attempting to use ALTER DOMAIN on non-domain types.

## Parameters / Member Variables
- : List of strings representing the qualified or unqualified type name to be moved
- : String name of the target schema where the type should be moved
- : ObjectType enum indicating whether this is a general type or domain (used for validation)
- : Output parameter that receives the OID of the original schema (can be NULL if not needed)

## Dependencies
- Functions called/Symbols referenced:
  - makeTypeNameFromNameList
  - typenameTypeId
  - get_typtype
  - LookupCreationNamespace
  - AlterTypeNamespace_oid
  - new_object_addresses
  - free_object_addresses
  - ObjectAddressSet
- Called from (representative examples):
  - ExecAlterObjectSchemaStmt

## Notes and Other Information
- Performs domain-specific validation when objecttype is OBJECT_DOMAIN, ensuring the target type is actually a domain type
- Uses temporary ObjectAddresses structure to track moved objects during the operation
- Returns an ObjectAddress pointing to the moved type for further processing by the caller
- Acts as a high-level wrapper around AlterTypeNamespace_oid, handling name resolution and validation