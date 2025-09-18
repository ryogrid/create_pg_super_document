# flush_pipe_input

## Location
src/backend/postmaster/syslogger.c: 1043 - 1093

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