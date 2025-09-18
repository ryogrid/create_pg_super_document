# objectsInSchemaToOids

## Location
src/backend/catalog/aclchk.c: 849 - 937

## Overview
Finds all objects of a specified type within given schemas and returns a list of their OIDs, with USAGE privilege checking on the schemas but no privilege checking on individual objects.

## Definition


## Detailed Description
This function iterates through a list of schema names and collects all objects of a specified type from those schemas. It performs USAGE privilege checks on each schema via LookupExplicitNamespace but does not check privileges on the individual objects found. The function handles different object types including tables (regular tables, views, materialized views, foreign tables, partitioned tables), sequences, and callable objects (functions, procedures, routines). For relation-based objects, it uses getRelationsInNamespace to efficiently retrieve objects by relation kind. For functions and procedures, it performs a catalog scan on pg_proc with appropriate filtering based on the prokind attribute.

## Parameters / Member Variables
- : The type of database object to search for (OBJECT_TABLE, OBJECT_SEQUENCE, OBJECT_FUNCTION, OBJECT_PROCEDURE, or OBJECT_ROUTINE)
- : A list of schema names (as String values) to search within

## Dependencies
- Functions called/Symbols referenced:
  - [LookupExplicitNamespace](../L/LookupExplicitNamespace.md)
  - [getRelationsInNamespace](../g/getRelationsInNamespace.md)
  - [list_concat](../l/list_concat.md)
  - lappend_oid
  - table_open
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md)
  - [heap_getnext](../h/heap_getnext.md)
  - [table_endscan](../t/table_endscan.md)
  - table_close
- Called from (representative examples):
  - InternalDefaultACL
  - [ExecuteGrantStmt](../E/ExecuteGrantStmt.md)

## Notes and Other Information
- The function is static and used internally within aclchk.c for ACL-related operations
- For OBJECT_TABLE, it collects multiple relation kinds: regular tables, views, materialized views, foreign tables, and partitioned tables
- For callable objects (functions/procedures), it distinguishes between different prokind values to filter appropriately
- OBJECT_ROUTINE includes both functions and procedures without filtering by prokind
- Error handling includes an elog(ERROR) for unrecognized object types
- The function concatenates results from multiple schemas into a single list