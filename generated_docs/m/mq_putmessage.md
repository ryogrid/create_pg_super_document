# mq_putmessage

## Location
src/backend/libpq/pqmq.c: 118 - 198

## Overview
Transmits a libpq protocol message to a shared memory message queue, handling blocking operations and signaling the receiver process appropriately.

## Definition
```c
static int mq_putmessage(char msgtype, const char *s, size_t len)
```

## Detailed Description
The mq_putmessage function is the core message transmission function for PostgreSQL's shared memory message queue communication system. It implements the putmessage method of the PQcommMethods interface specifically for shared memory queues (shm_mq).

The function constructs a message by combining a message type byte with the message data and sends it through the shared memory queue using vectored I/O (shm_mq_sendv). If the queue becomes full, the function will block and wait on a latch until space becomes available. It handles interrupts gracefully by detaching from the queue if an interrupt occurs while already busy sending a message.

The function also handles signaling the receiver process (typically the parallel leader) to notify it that a message is available. It distinguishes between regular parallel workers and logical parallel apply workers, sending different signal types accordingly.

Key behaviors include:
- Prevention of recursive message sending through the pq_mq_busy flag
- Graceful handling of detached or NULL message queues
- Blocking with interruptible waits when the queue is full
- Automatic notification of the receiver process

## Parameters / Member Variables
- `msgtype`: Single character indicating the type of PostgreSQL protocol message
- `s`: Pointer to the message data buffer
- `len`: Length of the message data in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [shm_mq_detach](../s/shm_mq_detach.md): Detaches from the shared memory queue
  - [shm_mq_sendv](../s/shm_mq_sendv.md): Sends vectored data to the shared memory queue
  - [IsLogicalParallelApplyWorker](../I/IsLogicalParallelApplyWorker.md): Checks if current process is a logical parallel apply worker
  - IsParallelWorker: Checks if current process is a parallel worker
  - [SendProcSignal](../S/SendProcSignal.md): Sends process signals to notify the receiver
  - [WaitLatch](../W/WaitLatch.md): Waits on a latch when queue is full
  - [ResetLatch](../R/ResetLatch.md): Resets the latch after waiting
  - CHECK_FOR_INTERRUPTS: Processes pending interrupts
- Called from (representative examples):
  - Accessed through PqCommMqMethods.putmessage function pointer
  - Used by the libpq protocol message sending infrastructure

## Notes and Other Information
- Located in src/backend/libpq/pqmq.c at lines 118-198
- Uses vectored I/O (shm_mq_iovec) to send the message type and data in a single operation
- The function sets pq_mq_busy to prevent recursive calls during message transmission
- Returns 0 on success, EOF on failure or when queue is detached
- Handles both regular parallel workers and logical parallel apply workers differently
- The force_flush parameter is always set to true when calling shm_mq_sendv for immediate notification
- This is a static function, not directly callable from outside pqmq.c