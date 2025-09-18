# mq_putmessage_noblock

## Location
[src/backend/libpq/pqmq.c:199-215](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqmq.c#L199-L215)

## Overview
A non-blocking message transmission function that is currently not implemented and throws an error when called.

## Definition
```c
static void mq_putmessage_noblock(char msgtype, const char *s, size_t len)
```

## Detailed Description
The mq_putmessage_noblock function is designed to be the non-blocking counterpart to mq_putmessage for shared memory message queue communication. However, it is currently unimplemented and serves as a placeholder in the PqCommMqMethods structure.

The function exists to satisfy the PQcommMethods interface requirement for a putmessage_noblock method, but the current implementation simply throws an ERROR with the message "not currently supported". 

The comment in the code explains that while the underlying shm_mq machinery does support non-blocking message sending, there is no current mechanism to start sending a message without committing to completing the entire transmission. This design limitation prevents the implementation of true non-blocking behavior where a partial send could be retried later.

This function represents a future enhancement opportunity where non-blocking message transmission could be implemented if the shared memory queue infrastructure is extended to support partial message transmission and retry mechanisms.

## Parameters / Member Variables
- `msgtype`: Single character indicating the type of PostgreSQL protocol message (unused in current implementation)
- `s`: Pointer to the message data buffer (unused in current implementation)  
- `len`: Length of the message data in bytes (unused in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - elog: Used to throw an ERROR with the "not currently supported" message
- Called from (representative examples):
  - Accessed through PqCommMqMethods.putmessage_noblock function pointer
  - Would be used in contexts requiring non-blocking message transmission

## Notes and Other Information
- Located in src/backend/libpq/pqmq.c at lines 199-215
- This function always throws an ERROR and never returns normally
- The function signature matches the PQcommMethods interface requirement
- Implementation is deferred due to limitations in the current shm_mq infrastructure
- The function has void return type unlike mq_putmessage which returns int
- This is a static function, not directly callable from outside pqmq.c
- Future implementation would require changes to the underlying shared memory queue system