# getInherits

## Location
[src/bin/pg_dump/pg_dump.c:7317-7372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L7317-L7372)

## Overview
Reads all inheritance information from the PostgreSQL system catalogs and returns it in an array of InhInfo structures for use in pg_dump operations.

## Definition

```c
InhInfo *
getInherits(Archive *fout, int *numInherits)
```
## Detailed Description
The getInherits function queries the pg_inherits system catalog to retrieve all table inheritance relationships in the database. It executes a simple SQL query to fetch inheritance pairs (child table OID and parent table OID) and stores them in an array of InhInfo structures. This information is essential for pg_dump to properly handle table inheritance when creating database dumps, ensuring that inheritance relationships are preserved during backup and restore operations.

## Parameters / Member Variables
- : Archive pointer representing the output destination for the dump operation
- : Output parameter that receives the total number of inheritance relationships found

## Dependencies
- Functions called/Symbols referenced:
  - [InhInfo](../I/InhInfo.md) (structure type)
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - PGRES_TUPLES_OK (constant)
  - pg_malloc
  - atooid
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)
  - [SubRelInfo](../S/SubRelInfo.md) (referenced in header)

## Notes and Other Information
- The function queries the pg_inherits catalog table directly using a simple SELECT statement
- Memory for the InhInfo array is allocated dynamically based on the number of tuples returned
- Each InhInfo structure contains inhrelid (child table OID) and inhparent (parent table OID)
- This function is part of the pg_dump utility's schema gathering phase
- The returned array should be freed by the caller when no longer needed