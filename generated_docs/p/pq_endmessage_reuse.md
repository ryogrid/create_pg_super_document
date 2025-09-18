# pq_endmessage_reuse

## Location
src/backend/libpq/pqformat.c: 314 - 325

## Overview
Sends a completed message to the frontend while preserving the data buffer for potential reuse, providing an efficient alternative to the standard pq_endmessage function.

## Definition


## Detailed Description
The  function completes and sends a message to the PostgreSQL frontend without freeing the underlying data buffer. This function is designed to work in tandem with  to enable efficient buffer reuse patterns, particularly useful when sending multiple similar messages where the buffer can be reused to avoid repeated memory allocation and deallocation.

The function retrieves the message type from the buffer's cursor field (where it was previously stored) and calls  to transmit the complete message. Unlike , this function intentionally preserves the buffer's memory allocation for subsequent reuse.

## Parameters / Member Variables
- : A StringInfo buffer containing the message data to be sent, with the message type stored in the cursor field

## Dependencies
- Functions called/Symbols referenced:
  - pq_putmessage
- Called from (representative examples):
  - [SendRowDescriptionMessage](../S/SendRowDescriptionMessage.md) (src/backend/access/common/printtup.c:243)
  - [printtup](printtup.md) (src/backend/access/common/printtup.c:375)
  - [UploadManifest](../U/UploadManifest.md) (src/backend/replication/walsender.c:707)
  - [exec_describe_statement_message](../e/exec_describe_statement_message.md) (src/backend/tcop/postgres.c:2691)

## Notes and Other Information
- This function is part of an optimization pattern for high-frequency message sending scenarios
- Must be used in conjunction with  for proper buffer management
- The message type is cleverly stored in the cursor field to avoid requiring an additional parameter
- Particularly useful in scenarios like result set transmission where many similar messages are sent sequentially
- The buffer remains allocated and ready for immediate reuse after this function completes