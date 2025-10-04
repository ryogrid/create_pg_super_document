# mq_putmessage

## Location
[src/backend/libpq/pqmq.c:118-198](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L118-L198)

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

## Simplified Source
```c
static int mq_putmessage(char msgtype, const char *s, size_t len) {
    shm_mq_iovec iov[2];
    shm_mq_result result;

    // Prevent recursive calls - detach queue if already busy
    if (pq_mq_busy) {
        if (pq_mq_handle != NULL)
            shm_mq_detach(pq_mq_handle);
        pq_mq_handle = NULL;
        return EOF;
    }

    // Handle already detached queue gracefully
    if (pq_mq_handle == NULL)
        return 0;

    pq_mq_busy = true;

    // Setup vectored I/O: message type + data
    iov[0].data = &msgtype;
    iov[0].len = 1;
    iov[1].data = s;
    iov[1].len = len;

    // Send message, wait if queue is full
    for (;;) {
        // Send with immediate flush for notification
        result = shm_mq_sendv(pq_mq_handle, iov, 2, true, true);

        // Signal receiver process about new message
        if (pq_mq_parallel_leader_pid != 0) {
            if (IsLogicalParallelApplyWorker())
                SendProcSignal(pq_mq_parallel_leader_pid,
                              PROCSIG_PARALLEL_APPLY_MESSAGE,
                              pq_mq_parallel_leader_proc_number);
            else
                SendProcSignal(pq_mq_parallel_leader_pid,
                              PROCSIG_PARALLEL_MESSAGE,
                              pq_mq_parallel_leader_proc_number);
        }

        // Exit if sent successfully or queue detached
        if (result != SHM_MQ_WOULD_BLOCK)
            break;

        // Wait for queue space, handle interrupts
        WaitLatch(MyLatch, WL_LATCH_SET | WL_EXIT_ON_PM_DEATH, 0,
                  WAIT_EVENT_MESSAGE_QUEUE_PUT_MESSAGE);
        ResetLatch(MyLatch);
        CHECK_FOR_INTERRUPTS();
    }

    pq_mq_busy = false;
    return (result == SHM_MQ_SUCCESS) ? 0 : EOF;
}
```