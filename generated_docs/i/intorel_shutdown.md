# intorel_shutdown

## Location
[src/backend/commands/createas.c:607-626](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/createas.c#L607-L626)

## Overview
intorel_shutdown performs cleanup and finalization tasks when tuple processing completes for CREATE TABLE AS and CREATE MATERIALIZED VIEW operations.

## Definition
static void intorel_shutdown(DestReceiver *self)

## Detailed Description
This function serves as the shutdown callback for DR_intorel destination receivers, called when the executor finishes processing all tuples. It performs essential cleanup operations including freeing the bulk insertion state and finalizing the bulk insert operation on the target relation. The function respects the skipData option - cleanup only occurs if data was actually inserted. After cleanup, it closes the target relation while maintaining the lock until transaction commit to ensure data integrity and prevent concurrent access during the ongoing transaction.

## Parameters / Member Variables
- : The DestReceiver object cast to DR_intorel containing the target relation and bulk insertion state

## Dependencies
- Functions called/Symbols referenced:
  - [FreeBulkInsertState](../F/FreeBulkInsertState.md)
  - table_finish_bulk_insert
  - table_close
- Called from (representative examples):
  - [CreateIntoRelDestReceiver](../C/CreateIntoRelDestReceiver.md) (sets as callback)
  - Executor shutdown sequence

## Notes and Other Information
The function sets the relation pointer to NULL after closing to prevent accidental reuse. The lock acquired during intorel_startup is retained until transaction commit, following PostgreSQL's standard locking protocol for DDL operations. This ensures that the newly created relation remains protected from concurrent access until the creating transaction commits. The bulk insertion state cleanup is essential for releasing memory and finalizing any buffered writes to storage.