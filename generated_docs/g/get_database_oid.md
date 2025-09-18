# get_database_oid

## Location
[src/backend/commands/dbcommands.c:3127-3173](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/dbcommands.c#L3127-L3173)

## Overview
Looks up a database's OID given its name, with optional error handling for missing databases.

## Definition


## Detailed Description
This function performs a name-to-OID lookup for databases in PostgreSQL's system catalog. Unlike many other system objects, there is no syscache (system cache) for pg_database indexed by name, so this function must perform a direct table scan of the pg_database system catalog to locate the database by name.

The function uses PostgreSQL's system table scanning infrastructure to search for a database with the specified name. It constructs a scan key to match against the  column and performs an indexed scan using the database name index for efficiency.

The  parameter controls error handling behavior: when , the function throws an error if the database doesn't exist, making it suitable for operations that require the database to exist. When , it returns  for missing databases, allowing callers to handle the absence gracefully.

## Parameters / Member Variables
- : The name of the database to look up
- : If , throw an error for missing databases; if , return  instead

## Dependencies
- Functions called/Symbols referenced:
  -  - Open the pg_database system catalog
  -  - [Initialize](../I/Initialize.md) scan key for database name lookup
  -  - Begin system table scan with index
  -  - Get next tuple from system scan
  -  - Check if tuple is valid
  -  - End system table scan
  -  - Close the catalog table
  -  - Check if OID is valid
  -  - Report error for missing database
  -  - Convert C string to Datum
- Types and constants referenced:
  -  - Structure for database catalog entries
  -  - OID of the pg_database catalog
  -  - OID of the database name index
  -  - Lock level for reading
  -  - Special OID value indicating no object
  -  - Error code for missing database
- Called from (representative examples):
  -  - Database creation operations
  -  - Database rename operations
  -  - Database parameter modification
  -  - Database size calculation
  -  - Object name resolution
  - Various ACL and permission checking functions

## Notes and Other Information
- Unlike many PostgreSQL system objects, databases don't have a syscache indexed by name, requiring direct table scanning
- Uses indexed scanning via  for better performance than sequential scanning
- The function assumes at most one matching tuple can exist (database names are unique)
- Error message format follows PostgreSQL standards: 
- Uses  to allow concurrent reads while preventing schema changes
- The function is defined in  and implemented in 
- Commonly used throughout PostgreSQL for resolving database names in DDL operations, ACL checks, and administrative functions
- Critical for database-related operations that need to validate database existence or convert names to internal identifiers