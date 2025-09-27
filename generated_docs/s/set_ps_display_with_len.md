# set_ps_display_with_len

## Location
[src/backend/utils/misc/ps_status.c:451-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/ps_status.c#L451-L485)

## Overview
Updates the process status display to show a combination of a fixed prefix and a specified activity string, with the activity length pre-calculated for performance optimization.

## Definition

```c
void
set_ps_display_with_len(const char *activity, size_t len)
```
## Detailed Description
This function updates the process title by combining a fixed prefix (typically set during initialization) with a current activity description. It is designed for performance-sensitive code paths where the activity string length is already known, avoiding redundant strlen() calls.

The function operates by:
- Validating that the provided length matches the activity string length (via Assert)
- Performing prerequisite checks using update_ps_display_precheck()
- Clearing any existing suffix state (ps_buffer_nosuffix_len = 0)
- Copying the activity string after the fixed prefix portion of ps_buffer
- Handling buffer overflow by truncating the activity if necessary
- Updating the actual process title through flush_ps_display()

This is a lower-level function used by higher-level process status functions and performance-critical code paths like query execution.

## Parameters / Member Variables
- : A null-terminated string describing the current process activity
- : The length of the activity string (must equal strlen(activity))

## Dependencies
- Functions called/Symbols referenced:
  - [update_ps_display_precheck](../u/update_ps_display_precheck.md) (prerequisite validation)
  - [flush_ps_display](../f/flush_ps_display.md) (applies the title change to the system)
  - strlen, memcpy (string manipulation functions)
  - Assert (debugging assertion macro)
- Called from (representative examples):
  - [exec_simple_query](../e/exec_simple_query.md) (src/backend/tcop/postgres.c:1126)
  - [exec_execute_message](../e/exec_execute_message.md) (src/backend/tcop/postgres.c:2180)  
  - [set_ps_display](set_ps_display.md) (src/include/utils/ps_status.h:42 - macro wrapper)

## Notes and Other Information
- The len parameter is provided as an optimization for callers that already know the string length
- Protected by PS_USE_NONE compilation flag - becomes a no-op when process status display is disabled
- Handles buffer overflow gracefully by truncating the activity string to fit available space
- Resets suffix tracking (ps_buffer_nosuffix_len = 0) since this represents a complete title change
- Uses ps_buffer_fixed_size to determine where the activity portion begins in the buffer
- Commonly used in query processing paths where process titles change frequently and performance matters

## Simplified Source

```c
// Simplified version of set_ps_display_with_len
void set_ps_display_with_len(const char *activity, size_t len) {
    // Validate input length matches actual string length
    Assert(strlen(activity) == len);

#ifndef PS_USE_NONE
    // Check if process title updates are enabled/needed
    if (!update_ps_display_precheck()) {
        return;
    }

    // Clear any existing suffix state for complete title change
    ps_buffer_nosuffix_len = 0;

    // Copy activity string after the fixed prefix in ps_buffer
    if (ps_buffer_fixed_size + len >= ps_buffer_size) {
        // Buffer overflow: truncate activity to fit available space
        memcpy(ps_buffer + ps_buffer_fixed_size, activity,
               ps_buffer_size - ps_buffer_fixed_size - 1);
        ps_buffer[ps_buffer_size - 1] = '\0';
        ps_buffer_cur_len = ps_buffer_size - 1;
    } else {
        // Normal case: copy complete activity string
        memcpy(ps_buffer + ps_buffer_fixed_size, activity, len + 1);
        ps_buffer_cur_len = ps_buffer_fixed_size + len;
    }

    // Update the actual process title in the system
    flush_ps_display();
#endif
}
```

Key simplifications made:
- Added descriptive comments explaining each major step
- Condensed buffer overflow handling logic with clear explanations
- Focused on the main execution path while preserving all essential logic
- Maintained the conditional compilation structure
- Preserved all critical assertions and buffer management