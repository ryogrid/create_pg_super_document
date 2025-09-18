# StartTransaction

## Location
[src/bin/pg_dump/pg_backup_db.c:529-536](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_db.c#L529-L536)

## Overview
A simple wrapper function that initiates a database transaction by executing a BEGIN statement during pg_dump restore operations.

## Definition
void StartTransaction(Archive *AHX)

## Detailed Description
StartTransaction is a straightforward utility function in the pg_dump restoration system that starts a database transaction by executing a BEGIN SQL command. This function serves as an abstraction layer for transaction management during database restoration operations. It uses the ExecuteSqlCommand function to send the BEGIN statement to the connected PostgreSQL server, providing a consistent interface for transaction control within the pg_dump ecosystem.

## Parameters / Member Variables
- `AHX`: Archive pointer (cast to ArchiveHandle internally) containing the database connection

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlCommand](../E/ExecuteSqlCommand.md)
- Called from (representative examples):
  - Various restoration functions that need transaction control

## Notes and Other Information
- This is a public function available through the pg_backup_db.h interface
- Provides transaction isolation for restore operations that require atomicity
- Paired with CommitTransaction function for complete transaction lifecycle management
- Error handling is delegated to ExecuteSqlCommand which will report any transaction start failures
- Essential for ensuring data consistency during complex restoration operations