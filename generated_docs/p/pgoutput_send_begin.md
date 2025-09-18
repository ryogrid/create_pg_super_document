# pgoutput_send_begin

## Location
[src/backend/replication/pgoutput/pgoutput.c:588-609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/pgoutput/pgoutput.c#L588-L609)

## Overview
Sends a BEGIN message for a logical replication transaction, initiating the transaction stream to the subscriber.

## Definition


## Detailed Description
This function is responsible for sending the BEGIN message that marks the start of a transaction in the logical replication stream. It is called while processing the first change of a transaction and ensures that the BEGIN message is sent exactly once per transaction. The function handles replication origin information if the transaction originated from a different node in a replication topology, and marks the transaction as having sent its BEGIN message to prevent duplicate sends.

## Parameters / Member Variables
- : LogicalDecodingContext pointer containing the output stream and plugin context
- : ReorderBufferTXN pointer representing the transaction being processed

## Dependencies
- Functions called/Symbols referenced:
  - [OutputPluginPrepareWrite](../O/OutputPluginPrepareWrite.md)
  - [logicalrep_write_begin](../l/logicalrep_write_begin.md)  
  - [send_repl_origin](../s/send_repl_origin.md)
  - [OutputPluginWrite](../O/OutputPluginWrite.md)
  - InvalidRepOriginId (constant)
  - [PGOutputTxnData](../P/PGOutputTxnData.md) (struct type)
- Called from (representative examples):
  - [pgoutput_change](pgoutput_change.md)
  - [pgoutput_truncate](pgoutput_truncate.md)
  - [pgoutput_message](pgoutput_message.md)

## Notes and Other Information
- This is a static function internal to the pgoutput plugin
- Uses assertions to ensure txndata exists and BEGIN hasn't been sent yet
- Conditionally includes replication origin information based on whether the transaction has a valid origin ID
- Sets the sent_begin_txn flag to true to prevent duplicate BEGIN messages
- Part of the logical replication output plugin infrastructure