# buildShSecLabels

## Location
src/bin/pg_dump/pg_dumpall.c: 1731 - 1756

## Overview
Builds SECURITY LABEL command(s) for shared database objects by querying the system catalog and formatting the results for SQL output.

## Definition


## Detailed Description
This function constructs PostgreSQL SECURITY LABEL commands for shared objects (objects that exist across the entire database cluster rather than within a specific database). It takes dual representations of the target object: catalog-level identification (catalog name and OID) for querying system catalogs, and SQL-level identification (object type and name) for generating the appropriate SECURITY LABEL commands.

The function operates by first building a query to retrieve security label information from the specified system catalog, executing that query, and then formatting the results into proper SECURITY LABEL SQL commands. This is part of pg_dumpall's capability to preserve security labels when dumping and restoring database clusters.

## Parameters / Member Variables
- : Active PostgreSQL database connection for executing queries
- : Name of the system catalog table (e.g., "pg_database", "pg_tablespace")
- : OID of the target object in the system catalog
- : SQL object type name for the SECURITY LABEL command (e.g., "DATABASE", "TABLESPACE")
- : Name of the object as it should appear in the SECURITY LABEL command (not pre-quoted)
- : Output buffer where the generated SECURITY LABEL commands will be appended

## Dependencies
- Functions called/Symbols referenced:
  - buildShSecLabelQuery (constructs the query to retrieve security labels)
  - executeQuery (executes the constructed query)
  - emitShSecLabels (formats query results into SECURITY LABEL commands)
- Called from (representative examples):
  - dumpRoles (in pg_dumpall.c at line 962)
  - dumpTablespaces (in pg_dumpall.c at line 1420)

## Notes and Other Information
- Handles shared objects that exist cluster-wide, not database-specific objects
- Requires dual object identification to bridge between catalog queries and SQL command generation  
- Part of pg_dumpall's comprehensive cluster backup functionality including security metadata
- The function assumes the caller provides consistent object identification between catalog and SQL representations
- Memory management handled internally - creates and destroys temporary query buffer
- Output commands are appended to the provided buffer, allowing integration with larger dump output