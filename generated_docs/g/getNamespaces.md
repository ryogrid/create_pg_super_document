# getNamespaces

## Location
src/bin/pg_dump/pg_dump.c: 5636 - 5753

## Overview
Reads all namespaces (schemas) from the PostgreSQL system catalogs and returns them as an array of NamespaceInfo structures for pg_dump processing.

## Definition


## Detailed Description
This function is a core part of pg_dump's metadata collection process. It queries the pg_namespace system catalog to retrieve information about all namespaces (schemas) in the database, including system schemas. Each namespace is converted into a NamespaceInfo structure containing the necessary metadata for dumping.

The function performs several important tasks:
1. Executes a SQL query to fetch namespace metadata including OID, name, owner, and ACL information
2. Creates NamespaceInfo structures for each namespace with proper dump object initialization
3. Determines which namespaces should be dumped based on dump configuration
4. Handles special ACL processing for the 'public' schema to ensure consistency across PostgreSQL versions
5. Sets up component flags to indicate whether ACL information should be dumped

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : Output parameter that receives the total number of namespaces found

## Dependencies
- Functions called/Symbols referenced:
  - ExecuteSqlQuery
  - atooid
  - AssignDumpId
  - getRoleName
  - selectDumpableNamespace
  - quoteAclUserName
  - appendPGArray
- Called from (representative examples):
  - getSchemaData

## Notes and Other Information
- Fetches ALL namespaces including system ones to ensure proper object linking
- Special handling for 'public' schema ACLs to maintain consistency across PostgreSQL versions
- Uses predetermined default ACLs for public schema rather than pg_init_privs entries
- Supports dump/reload of public schema ownership changes
- Sets DUMP_COMPONENT_ACL flag when namespaces have non-null ACL information
- Memory allocation uses pg_malloc for the NamespaceInfo array
- Returns allocated array that must be freed by caller