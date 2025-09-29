# XLogRegisterBufData

## Location
[src/backend/access/transam/xloginsert.c:405-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xloginsert.c#L405-L455)

## Overview
XLogRegisterBufData adds buffer-specific data to a WAL record being constructed, associating the data with a previously registered buffer block for replay purposes.

## Definition
void XLogRegisterBufData(uint8 block_id, char *data, uint32 len)

## Detailed Description
XLogRegisterBufData is a specialized function in PostgreSQL's WAL insertion system that allows registration of data specifically associated with a buffer block. Unlike XLogRegisterData which adds data to the main record chunk, this function associates data with a particular buffer block that was previously registered via XLogRegisterBuffer().

The function maintains strict limits on the amount of data that can be associated with each buffer block (maximum 65535 bytes per block). This limit ensures compatibility with the physical WAL record format where the data_length field in XLogRecordBlockHeader uses a uint16. The function supports multiple calls for the same block_id, appending additional data segments to that block's data chain.

The function validates that the specified block_id corresponds to an active registered buffer and maintains a linked list of data segments (rdata chain) associated with each buffer. It also enforces global limits on the total number of data segments to prevent resource exhaustion.

## Parameters / Member Variables
- : Identifier of the previously registered buffer block (must be valid and in use)
- : Pointer to the buffer-specific data to be included in the WAL record
- : Length of the data buffer in bytes (maximum 65535 bytes per block total)

## Dependencies
- Functions called/Symbols referenced:
  - [registered_buffer](../r/registered_buffer.md) (struct type for tracking registered buffers)
  - [XLogRecData](XLogRecData.md) (struct type for data segments)
  - [errdetail_internal](../e/errdetail_internal.md) (for error reporting)
  - elog (for error logging)
  - Assert (for debug assertions)
- Called from (representative examples):
  - [_bt_insertonpg](../b/_bt_insertonpg.md) (B-tree page modifications)
  - [heap_insert](../h/heap_insert.md) (heap tuple insertions with buffer data)
  - [log_heap_update](../l/log_heap_update.md) (heap update operations)
  - [_hash_doinsert](../h/_hash_doinsert.md) (hash index insertions)
  - [gistXLogUpdate](../g/gistXLogUpdate.md) (GiST index updates)

## Notes and Other Information
- Must be called after the corresponding XLogRegisterBuffer() call
- The block_id must reference a valid, registered buffer
- Maximum 65535 bytes of data can be registered per buffer block
- Multiple calls for the same block_id will append data segments
- Data will be available during replay via XLogRecGetBlockData()
- Commonly used for storing operation-specific details about buffer modifications
- The data pointer must remain valid until XLogInsert() is called

## Simplified Source

```c
// Simplified version of XLogRegisterBufData
void XLogRegisterBufData(uint8 block_id, char *data, uint32 len)
{
    registered_buffer *regbuf;
    XLogRecData *rdata;

    // Ensure WAL insertion has been initiated
    Assert(begininsert_called);

    // Find the registered buffer for this block_id
    regbuf = &registered_buffers[block_id];
    if (!regbuf->in_use)
        elog(ERROR, "no block with id %d registered with WAL insertion", block_id);

    // Check resource limits
    if (num_rdatas >= max_rdatas)
        ereport(ERROR, (errmsg_internal("too much WAL data")));

    // Check data size limits (max 65535 bytes per block)
    if (regbuf->rdata_len + len > UINT16_MAX || len > UINT16_MAX)
        ereport(ERROR, (errmsg_internal("too much WAL data for block %u", block_id)));

    // Create new data segment
    rdata = &rdatas[num_rdatas++];
    rdata->data = data;
    rdata->len = len;

    // Append to buffer's data chain
    regbuf->rdata_tail->next = rdata;
    regbuf->rdata_tail = rdata;
    regbuf->rdata_len += len;
}
```

Key simplifications made:
- Removed detailed error message formatting for clarity
- Consolidated error checking logic
- Simplified comments to focus on main operations
- Abstracted complex error detail construction
- Maintained essential validation and data chaining logic