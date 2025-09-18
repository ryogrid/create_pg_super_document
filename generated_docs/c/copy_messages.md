# copy_messages

## Location
src/test/modules/test_shm_mq/worker.c: 176 - 197

## Overview
The core message processing loop that receives messages from an input queue and forwards them to an output queue until the connection is broken.

## Definition
```c
static void copy_messages(shm_mq_handle *inqh, shm_mq_handle *outqh)
```

## Detailed Description
This function implements the main processing logic for a worker in the test_shm_mq parallel processing pipeline. It performs a simple message relay operation:

1. **Infinite Processing Loop**: Continuously processes messages until a queue operation fails
2. **Interrupt Handling**: Regularly checks for interrupts (SIGTERM, query cancellation, etc.) using CHECK_FOR_INTERRUPTS()
3. **Message Reception**: Receives messages from the input queue in non-blocking mode
4. **Message Forwarding**: Immediately sends received messages to the output queue
5. **Error Handling**: Breaks the loop when either receive or send operations fail (indicating connection loss or process termination)

The function represents the "real work" portion of the background worker, with everything before it being initialization and everything after being cleanup. In a real application, this is where custom message processing logic would be implemented instead of simple forwarding.

The non-blocking I/O approach ensures that the worker can respond promptly to interrupts and system shutdown requests.

## Parameters / Member Variables
- `inqh`: Handle to the input shared memory message queue from which to receive messages
- `outqh`: Handle to the output shared memory message queue to which messages should be forwarded

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (PostgreSQL interrupt processing macro)
  - shm_mq_receive (receive message from input queue)
  - shm_mq_send (send message to output queue)
  - SHM_MQ_SUCCESS (success result constant)
- Called from (representative examples):
  - test_shm_mq_main (after worker initialization is complete)

## Notes and Other Information
- This function is static (internal to worker.c) and represents the application-specific processing logic
- Uses non-blocking I/O (false parameter to shm_mq_receive) to ensure responsiveness
- The shm_mq_send call uses parameters (len, data, false, true) indicating non-blocking send with detach_when_done=true
- Function terminates gracefully when queue operations fail, typically due to coordinator shutdown or process termination
- In production applications, this function would be replaced with domain-specific message processing logic
- The simple copy operation demonstrates the message queue API usage pattern without adding complexity