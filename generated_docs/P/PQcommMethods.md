# PQcommMethods

## Location
[src/include/libpq/libpq.h:41-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq.h#L41-L44)

## Overview
PQcommMethods is a function pointer structure that provides an abstraction layer for PostgreSQL's backend communication methods, allowing the system to switch between different communication backends such as socket-based and message queue-based communication.

## Definition


## Detailed Description
PQcommMethods implements a strategy pattern for PostgreSQL's backend communication layer. It defines a set of function pointers that abstract the underlying communication mechanism between the PostgreSQL backend and clients. This design allows PostgreSQL to support different communication backends without changing the higher-level code that sends and receives messages.

The structure is used through a global pointer  that points to the active implementation. The system provides convenient macros (pq_flush, pq_putmessage, etc.) that delegate to the appropriate function through this pointer.

Two main implementations exist:
- Socket-based communication () for standard TCP connections
- Message queue communication () for parallel worker processes

## Parameters / Member Variables
- : Function to reset the communication state and prepare for new operations
- : Function to flush any buffered output data to the underlying transport
- : Function to flush output data only if the transport is ready for writing
- : Function that returns true if there are pending messages to be sent
- : Function to send a message with specified type, content, and length (blocking)
- : Function to send a message without blocking (queues for later transmission)

## Dependencies
- Functions called/Symbols referenced:
  - [socket_comm_reset](../s/socket_comm_reset.md)
  - socket_flush
  - [socket_flush_if_writable](../s/socket_flush_if_writable.md)
  - [socket_is_send_pending](../s/socket_is_send_pending.md)
  - [socket_putmessage](../s/socket_putmessage.md)
  - [socket_putmessage_noblock](../s/socket_putmessage_noblock.md)
  - PqCommMqMethods (alternative implementation)
- Called from (representative examples):
  - pq_flush (macro)
  - pq_putmessage (macro)
  - pq_flush_if_writable (macro)
  - pq_is_send_pending (macro)

## Notes and Other Information
- The global variable  is defined in pqcomm.c and defaults to 
- The structure enables runtime switching of communication backends, particularly useful for parallel query execution
- All access to these functions should go through the provided macros (pq_flush, pq_putmessage, etc.) rather than direct function pointer calls
- The message queue implementation is used specifically for communication between parallel worker processes
- This abstraction is part of PostgreSQL's libpq backend communication infrastructure