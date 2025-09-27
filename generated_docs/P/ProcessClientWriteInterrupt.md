# ProcessClientWriteInterrupt

## Location
[src/backend/tcop/postgres.c:559-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/postgres.c#L559-L614)

## Overview
Processes interrupts specific to client writes, handling process termination requests during write operations while preventing protocol synchronization issues.

## Definition
```c
void ProcessClientWriteInterrupt(bool blocked)
```

## Detailed Description
ProcessClientWriteInterrupt is a specialized interrupt handler for client write operations in PostgreSQL. Unlike its read counterpart, this function focuses primarily on handling process termination requests (ProcDiePending) during write operations. The function is designed to prevent indefinite delays when a client connection becomes stuck during writes, while also maintaining protocol synchronization integrity.

When the process is dying and the write operation is blocked, the function carefully manages the output destination to avoid sending error messages that could cause additional blocking or protocol sync loss. It only processes interrupts when it's safe to do so (checking InterruptHoldoffCount and CritSectionCount), and redirects output from DestRemote to DestNone to prevent further client communication attempts.

## Parameters / Member Variables
- `blocked`: Boolean indicating whether no data could be written and the operation will retry (true), or if the function is called before writing or after completing a write (false)

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS (macro)
  - [SetLatch](../S/SetLatch.md)
  - DestRemote (constant)
  - DestNone (constant)
- Called from (representative examples):
  - [secure_write](../s/secure_write.md) (in be-secure.c)

## Notes and Other Information
- Must preserve errno value across the function call
- Primarily focused on handling process termination during write operations
- Prevents protocol synchronization issues by avoiding error message transmission when terminating
- More restrictive than ProcessClientReadInterrupt, only handling specific interrupt conditions
- Critical for preventing stuck client connections from indefinitely delaying server shutdown
- Uses whereToSendOutput redirection to prevent problematic client communication during termination

## Simplified Source

```c
// Simplified version of ProcessClientWriteInterrupt
void ProcessClientWriteInterrupt(bool blocked) {
    // Preserve errno value across function execution
    int saved_errno = errno;

    // Check if process is scheduled to die
    if (ProcDiePending) {
        if (blocked) {
            // Write is blocked - handle termination if safe to do so
            if (InterruptHoldoffCount == 0 && CritSectionCount == 0) {
                // Redirect output to prevent protocol sync issues
                if (whereToSendOutput == DestRemote) {
                    whereToSendOutput = DestNone;
                }

                // Process the termination request
                CHECK_FOR_INTERRUPTS();
            }
        } else {
            // About to write or done writing - ensure latch is set
            // so we can respond to termination signal promptly
            SetLatch(MyLatch);
        }
    }

    // Restore original errno value
    errno = saved_errno;
}
```

Key simplifications made:
- Added descriptive comments explaining the logic flow
- Used more descriptive variable name (saved_errno vs save_errno)
- Clarified the two main execution paths (blocked vs not blocked)
- Preserved critical safety checks and protocol handling
- Maintained errno preservation requirement