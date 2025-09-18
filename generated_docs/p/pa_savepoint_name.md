# pa_savepoint_name

## Location
[src/backend/replication/logical/applyparallelworker.c:1348-1361](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/applyparallelworker.c#L1348-L1361)

## Overview
pa_savepoint_name is a utility function that generates unique savepoint names for streaming transactions in PostgreSQL's parallel logical replication system.

## Definition
static void pa_savepoint_name(Oid suboid, TransactionId xid, char *spname, Size szsp)

## Detailed Description
This function creates unique savepoint names specifically for streaming transactions in the parallel apply worker context. The function addresses a critical requirement in logical replication: ensuring savepoint name uniqueness across multiple subscriptions that might receive the same remote transaction ID.

The function constructs savepoint names using the format "pg_sp_<subscription_oid>_<transaction_id>". This naming scheme ensures uniqueness by incorporating both the subscription OID and the transaction ID, preventing conflicts when different subscriptions from different nodes happen to use the same remote transaction ID.

The function uses snprintf to safely format the savepoint name into the provided buffer, respecting the buffer size limit to prevent buffer overflow conditions. Being declared as static, this function is internal to the applyparallelworker.c module.

## Parameters / Member Variables
- : The OID (Object Identifier) of the subscription, used to ensure uniqueness across different subscriptions
- : The transaction ID from the remote system, which may not be globally unique across different publications/nodes
- : Character buffer where the generated savepoint name will be stored
- : Size of the spname buffer, used to prevent buffer overflow in snprintf

## Dependencies
- Functions called/Symbols referenced:
  - snprintf (standard C library function for safe string formatting)
  - Oid (PostgreSQL object identifier type)
  - TransactionId (PostgreSQL transaction identifier type)
  - Size (PostgreSQL size type)
- Called from (representative examples):
  - [pa_start_subtrans](pa_start_subtrans.md)
  - [pa_stream_abort](pa_stream_abort.md)

## Notes and Other Information
- This function is located in src/backend/replication/logical/applyparallelworker.c:1348-1361
- Declared as static, making it internal to the applyparallelworker.c module
- The naming convention "pg_sp_" indicates these are PostgreSQL-internal savepoints for streaming transactions
- Critical for handling transaction isolation in parallel logical replication scenarios
- The combination of subscription OID and transaction ID ensures global uniqueness even when multiple subscriptions process transactions with identical remote XIDs
- Uses snprintf for buffer safety, preventing potential security issues from buffer overflows