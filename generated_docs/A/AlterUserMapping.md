# AlterUserMapping

## Location
src/backend/commands/foreigncmds.c: 1237 - 1334

## Overview
Modifies the options and configuration of an existing user mapping for accessing a foreign server in PostgreSQL's foreign data wrapper system.

## Definition
```c
ObjectAddress AlterUserMapping(AlterUserMappingStmt *stmt)
```

## Detailed Description
This function implements the ALTER USER MAPPING SQL command by updating an existing entry in the pg_user_mapping system catalog. It allows users to modify connection options and authentication parameters for existing foreign server mappings without recreating them. The function performs comprehensive validation including existence checks, permission verification through the same access control mechanisms as other user mapping operations, and option validation through the foreign data wrapper's validator function. The function supports both regular users and the special PUBLIC role, and uses PostgreSQL's standard tuple modification mechanisms to update the catalog entry.

## Parameters / Member Variables
- `stmt`: Pointer to AlterUserMappingStmt structure containing the parsed ALTER USER MAPPING command details including user specification, server name, and new options to be applied

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - get_rolespec_oid
  - GetForeignServerByName
  - GetSysCacheOid2
  - user_mapping_ddl_aclcheck
  - SearchSysCacheCopy1
  - GetForeignDataWrapper
  - SysCacheGetAttr
  - transformGenericOptions
  - heap_modify_tuple
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
  - heap_freetuple
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
The function performs in-place modification of the user mapping's options by retrieving the existing tuple, modifying only the options column, and updating the catalog. It properly handles NULL options by using replacement arrays to control which columns are updated. The function maintains all existing dependencies and relationships while allowing option changes, making it safe for ongoing foreign data wrapper operations.