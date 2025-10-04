# verify_message

## Location
[src/test/modules/test_shm_mq/test.c:258-273](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_shm_mq/test.c#L258-L273)

## Overview
A utility function that performs byte-by-byte comparison between two messages to verify data integrity in shared memory message queue tests.

## Definition
```c
static void verify_message(Size origlen, char *origdata, Size newlen, char *newdata)
```

## Detailed Description
This function provides message integrity verification by comparing an original message with a received message to ensure no data corruption occurred during transmission through shared memory queues. It performs two levels of validation:

1. **Length Validation**: Compares the sizes of the original and received messages
2. **Content Validation**: Performs byte-by-byte comparison of the message data

If any discrepancy is found, the function reports a detailed error indicating the nature of the corruption. For content mismatches, it provides the exact byte offset where the difference was detected, making debugging easier.

## Parameters / Member Variables
- `origlen` (Size): Length of the original message in bytes
- `origdata` (char*): Pointer to the original message data
- `newlen` (Size): Length of the received message in bytes  
- `newdata` (char*): Pointer to the received message data

## Dependencies
- Functions called/Symbols referenced:
  - ereport: PostgreSQL's error reporting mechanism
  - [errmsg](../e/errmsg.md): Creates the main error message
  - [errdetail](../e/errdetail.md): Provides additional error detail information
- Called from (representative examples):
  - [test_shm_mq](../t/test_shm_mq.md): Uses verify_message to validate final message integrity after ring traversal
  - [test_shm_mq_pipelined](../t/test_shm_mq_pipelined.md): Optionally uses verify_message for each received message when verify=true

## Notes and Other Information
- Static function scope - only accessible within the test_shm_mq module
- Provides detailed error messages with specific corruption information (byte offset, lengths)
- Essential for validating that shared memory queue operations preserve data integrity
- Used in both basic and pipelined shared memory queue tests
- No return value - either succeeds silently or reports an error and aborts
- Critical for ensuring reliability of PostgreSQL's shared memory message queue infrastructure

## Simplified Source

```c
static void
verify_message(Size origlen, char *origdata, Size newlen, char *newdata)
{
    Size i;

    // Check if message lengths match
    if (origlen != newlen)
        ereport(ERROR,
                (errmsg("message corrupted"),
                 errdetail("The original message was %zu bytes but the final message is %zu bytes.",
                          origlen, newlen)));

    // Compare message content byte by byte
    for (i = 0; i < origlen; ++i)
        if (origdata[i] != newdata[i])
            ereport(ERROR,
                    (errmsg("message corrupted"),
                     errdetail("The new and original messages differ at byte %zu of %zu.", i, origlen)));
}
```