# shm_mq

## Location
[src/backend/storage/ipc/shm_mq.c:71-136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shm_mq.c#L71-L136)

## Overview
A shared memory message queue structure that enables efficient inter-process communication in PostgreSQL's parallel processing infrastructure. It provides a lock-free ring buffer for asynchronous message passing between sender and receiver processes.

## Definition

```c
struct shm_mq
{
	slock_t		mq_mutex;
	PGPROC	   *mq_receiver;
	PGPROC	   *mq_sender;
	pg_atomic_uint64 mq_bytes_read;
	pg_atomic_uint64 mq_bytes_written;
	Size		mq_ring_size;
	bool		mq_detached;
	uint8		mq_ring_offset;
	char		mq_ring[FLEXIBLE_ARRAY_MEMBER];
};
```
## Detailed Description
The  structure represents the actual message queue stored in shared memory, designed for high-performance communication between parallel processes in PostgreSQL. The implementation uses careful synchronization techniques to achieve lock-free operation for the critical path operations:

- **Atomic Operations**: Uses atomic 64-bit operations for  and  counters to avoid locking during data transfer
- **Memory Barriers**: Employs memory barriers to ensure proper ordering of ring buffer reads/writes with counter updates
- **Process Identification**: Tracks sender and receiver processes via PGPROC pointers, protected by mutex but immutable once set
- **Ring Buffer**: Implements a circular buffer () where the difference between bytes written and read determines available data

The design allows the sender to write to unused portions and the receiver to read unread bytes without coordination, maximizing throughput while maintaining data integrity.

## Parameters / Member Variables
- `mq_mutex`: Spinlock protecting mq_receiver and mq_sender fields during initialization
- `*mq_receiver`: Pointer to the PGPROC of the receiving process (immutable once set)
- `*mq_sender`: Pointer to the PGPROC of the sending process (immutable once set)
- `mq_bytes_read`: Atomic counter tracking total bytes consumed by receiver
- `mq_bytes_written`: Atomic counter tracking total bytes produced by sender
- `mq_ring_size`: Size of the circular buffer (immutable after initialization)
- `mq_detached`: Boolean flag indicating if queue is detached (can only transition false→true)
- `mq_ring_offset`: Offset alignment for the ring buffer data
- `mq_ring[FLEXIBLE_ARRAY_MEMBER]`: Flexible array member containing the actual ring buffer data

## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](slock_t.md)
  - [PGPROC](../P/PGPROC.md)
  - [pg_atomic_uint64](../p/pg_atomic_uint64.md)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [shm_mq_create](shm_mq_create.md)
  - [shm_mq_attach](shm_mq_attach.md)  
  - [shm_mq_sendv](shm_mq_sendv.md)
  - [shm_mq_receive](shm_mq_receive.md)
  - [ExecParallelSetupTupleQueues](../E/ExecParallelSetupTupleQueues.md)
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)

## Notes and Other Information
- Critical for PostgreSQL's parallel query execution and logical replication
- Lock-free design optimizes for high-throughput message passing scenarios
- Memory barriers and atomic operations ensure correctness on multi-core systems
- The detached flag provides a clean shutdown mechanism for both sender and receiver
- Ring buffer size is fixed at creation time and cannot be changed dynamically
- Used extensively in parallel workers, tuple queues, and parallel apply workers