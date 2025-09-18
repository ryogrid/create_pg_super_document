# GetCurrentSubTransactionId

## Location
src/backend/access/transam/xact.c: 788 - 801

## Overview
Returns the ID of the current subtransaction within the active transaction.

## Definition


## Detailed Description
GetCurrentSubTransactionId is a simple accessor function that returns the subtransaction ID of the current transaction state. It retrieves the subTransactionId field from the CurrentTransactionState global variable, which tracks the current transaction's state information. This function provides a way for other parts of the PostgreSQL system to identify which subtransaction context they are operating within.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - CurrentTransactionState (global variable)
  - TransactionState (struct type)
- Called from (representative examples):
  - CreateParallelContext (src/backend/access/transam/parallel.c:186)
  - InitTempTableNamespace (src/backend/catalog/namespace.c:4502)
  - CopyFrom (src/backend/commands/copyfrom.c:746-747)
  - ExecuteTruncateGuts (src/backend/commands/tablecmds.c:2072)
  - register_on_commit_action (src/backend/commands/tablecmds.c:17539)
  - SPI_connect_ext (src/backend/executor/spi.c:140)
  - AllocateFile (src/backend/storage/file/fd.c:2604)
  - CreatePortal (src/backend/utils/mmgr/portalmem.c:211)

## Notes and Other Information
- This function is part of PostgreSQL's subtransaction management system
- The returned SubTransactionId is used to track nested transaction contexts
- Located in src/backend/access/transam/xact.c:788-801
- Simple getter function with no side effects or error conditions