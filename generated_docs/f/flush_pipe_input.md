# flush_pipe_input

## Location
[src/backend/postmaster/syslogger.c:1043-1093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L1043-L1093)

## Overview
Forces out any buffered data from the syslogger pipe input, primarily used during syslogger shutdown to ensure all pending log data is written.

## Definition
```c
static void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
```

## Detailed Description
This function is responsible for flushing all buffered log data when the syslogger is shutting down or when forced output is needed. It operates in two phases: first, it processes any incomplete protocol messages stored in the buffer lists, and second, it forces out any remaining raw pipe data. The function is designed to leave the system in a clean state by marking buffers as unused and reclaiming string storage.

The function iterates through all buffer lists (NBUFFER_LISTS), examining each save_buffer entry. For buffers that contain active data (pid != 0), it writes the buffered content to stderr and then marks the buffer as unused by setting pid to 0 and freeing the associated string data.

## Parameters / Member Variables
- `logbuffer`: Character buffer containing raw pipe data that needs to be flushed
- `bytes_in_logbuffer`: Pointer to integer tracking the number of bytes currently in the log buffer; reset to 0 after flushing

## Dependencies
- Functions called/Symbols referenced:
  - [write_syslogger_file](../w/write_syslogger_file.md)
  - LOG_DESTINATION_STDERR
  - NBUFFER_LISTS
  - [save_buffer](../s/save_buffer.md)
- Called from (representative examples):
  - [SysLoggerMain](../S/SysLoggerMain.md)
  - [pipeThread](../p/pipeThread.md)

## Notes and Other Information
- This is a static function only used within the syslogger module
- Currently used primarily at syslogger shutdown but designed to be reusable
- Handles both structured protocol messages and raw pipe data
- Ensures proper cleanup by freeing allocated string storage and marking buffers as unused
- All output is directed to stderr during the flush operation

## Simplified Source

```c
// Simplified version of flush_pipe_input
static void flush_pipe_input(char *logbuffer, int *bytes_in_logbuffer) {
    // Phase 1: Process all incomplete protocol messages in buffer lists
    for (int i = 0; i < NBUFFER_LISTS; i++) {
        List *list = buffer_lists[i];
        ListCell *cell;

        foreach(cell, list) {
            save_buffer *buf = (save_buffer *) lfirst(cell);

            // If buffer contains active data, write it out
            if (buf->pid != 0) {
                StringInfo str = &(buf->data);

                // Write buffered content to stderr
                write_syslogger_file(str->data, str->len, LOG_DESTINATION_STDERR);

                // Clean up: mark buffer unused and free memory
                buf->pid = 0;
                pfree(str->data);
            }
        }
    }

    // Phase 2: Flush any remaining raw pipe data
    if (*bytes_in_logbuffer > 0) {
        write_syslogger_file(logbuffer, *bytes_in_logbuffer, LOG_DESTINATION_STDERR);
    }

    // Reset buffer counter
    *bytes_in_logbuffer = 0;
}
```

Key simplifications made:
- Combined variable declarations with initialization where possible
- Added phase comments to clarify the two-stage process
- Simplified comments to focus on core actions
- Maintained the essential logic flow and cleanup operations
- Preserved all critical functionality while improving readability