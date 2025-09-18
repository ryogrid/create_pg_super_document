# pgoutput_rollback_prepared_txn

## Location
[src/backend/replication/pgoutput/pgoutput.c:687-704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L687-L704)

## Overview
Handles the rollback of a prepared transaction in the pgoutput logical replication output plugin, writing the rollback information to the replication stream.

## Definition


## Detailed Description
This function is a callback handler for the ROLLBACK PREPARED operation in PostgreSQL's logical replication pgoutput plugin. When a prepared transaction is rolled back, this function is invoked to serialize the rollback information and send it to subscribers through the logical replication stream. The function follows the standard pattern of updating progress, preparing the output buffer, writing the rollback data using the logical replication protocol, and then committing the write operation.

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the replication context and output stream
- : ReorderBufferTXN pointer representing the transaction being rolled back
- : XLogRecPtr indicating the LSN where the prepare operation ended
- : TimestampTz representing when the transaction was originally prepared

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginUpdateProgress](../O/OutputPluginUpdateProgress.md)
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - logicalrep_write_rollback_prepared
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
  - [LogicalDecodingContext](../L/LogicalDecodingContext.md) (type)
  - [ReorderBufferTXN](../R/ReorderBufferTXN.md) (type)
- Called from (representative examples):
  - [_PG_output_plugin_init](../P/_PG_output_plugin_init.md) (registered as callback)

## Notes and Other Information
This function is registered as a callback in the pgoutput plugin initialization and is part of PostgreSQL's two-phase commit support in logical replication. It ensures that prepared transaction rollbacks are properly propagated to logical replication subscribers, maintaining consistency across the replication cluster. The function is static and only used within the pgoutput plugin module.