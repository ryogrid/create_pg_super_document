# initReadOnlyStringInfo

## Location
[src/include/lib/stringinfo.h:130-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/lib/stringinfo.h#L130-L147)

## Overview
initReadOnlyStringInfo initializes a StringInfoData struct from an existing string buffer without copying the data, optimized for performance-critical scenarios where buffer allocation and copying would be too costly.

## Definition


## Detailed Description
This function provides a lightweight initialization mechanism for StringInfoData structures when working with existing string buffers. It creates a read-only StringInfo that references external memory without taking ownership of the buffer. The function is designed for high-performance scenarios where the overhead of allocating new memory and copying data would be prohibitive.

The initialized StringInfo is marked as read-only by setting maxlen to 0, which prevents any modification operations. The caller retains full responsibility for the lifecycle of the underlying buffer, including ensuring it remains valid for the entire duration the StringInfo is in use.

## Parameters / Member Variables
- : Pointer to the StringInfoData structure to be initialized
- : Pointer to the existing string buffer; does not need to be palloc'd and may omit null termination
- : Length of the string data in the buffer

## Dependencies
- Functions called/Symbols referenced: (None)
- Called from (representative examples):
  - [LogicalParallelApplyLoop](../L/LogicalParallelApplyLoop.md) (src/backend/replication/logical/applyparallelworker.c:776)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (src/backend/replication/logical/worker.c:2091)
  - [XLogWalRcvProcessMsg](../X/XLogWalRcvProcessMsg.md) (src/backend/replication/walreceiver.c:860)
  - [exec_bind_message](../e/exec_bind_message.md) (src/backend/tcop/postgres.c:1849)
  - [ReadArrayBinary](../R/ReadArrayBinary.md) (src/backend/utils/adt/arrayfuncs.c:1496)

## Notes and Other Information
- The resulting StringInfo is read-only and cannot be used with appendStringInfo functions or resetStringInfo()
- The data parameter does not require null termination at data[len]
- The caller must ensure the referenced buffer remains valid throughout the StringInfo's lifetime
- Performance optimization is the primary use case, particularly in replication and binary data processing contexts
- maxlen is set to 0 to indicate read-only status