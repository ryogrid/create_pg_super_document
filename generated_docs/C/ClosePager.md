# ClosePager

## Location
[src/fe_utils/print.c:3141-3171](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3141-L3171)

## Overview
Closes a previously opened pager pipe and restores signal handling to its original state.

## Definition
```c
void ClosePager(FILE *pagerpipe)
```

## Detailed Description
This function safely closes a pager process that was previously opened by PageOutput. It first checks if the provided file pointer is valid and not stdout (indicating it's actually a pager pipe). If the user canceled printing midstream (detected via the cancel_pressed flag), it sends an "Interrupted" message to the pager before closing. The function then uses pclose() to properly terminate the pager process and restores the SIGPIPE signal trap to its previous state. This ensures proper cleanup of the pager subprocess and signal handling.

## Parameters / Member Variables
- `pagerpipe`: FILE pointer to the pager pipe that should be closed, or stdout if no pager is active

## Dependencies
- Functions called/Symbols referenced:
  - [pclose](../p/pclose.md) (close pipe to pager process)
  - [restore_sigpipe_trap](../r/restore_sigpipe_trap.md) (restore SIGPIPE signal handling)
- Called from (representative examples):
  - [exec_command_sf_sv](../e/exec_command_sf_sv.md) (src/bin/psql/command.c:2589)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1718, 1745)
  - [usage](../u/usage.md), slashUsage, helpVariables (various help functions)
  - [printHistory](../p/printHistory.md) (src/bin/psql/input.c:527)
  - [print_aligned_text](../p/print_aligned_text.md) (src/fe_utils/print.c:1220)
  - [printTable](../p/printTable.md) (src/fe_utils/print.c:3536)

## Notes and Other Information
- Safe to call with stdout - function will detect this and do nothing
- Handles user interruption (Ctrl-C) gracefully by sending an "Interrupted" message to the pager
- Properly manages pager process termination using pclose() rather than fclose()
- Restores SIGPIPE signal handling that was modified when the pager was opened
- Part of the paging infrastructure used throughout psql and other PostgreSQL frontend tools
- The interrupted message may not be visible if the pager itself terminated due to SIGINT

## Simplified Source

```c
void
ClosePager(FILE *pagerpipe)
{
    // Only close if it's actually a pager pipe (not stdout)
    if (pagerpipe && pagerpipe != stdout) {
        // If user interrupted printing, notify pager
        // (some pagers like 'less' use Ctrl-C as part of their command set)
        if (cancel_pressed)
            fprintf(pagerpipe, _("Interrupted\n"));

        // Close the pager process and restore signal handling
        pclose(pagerpipe);
        restore_sigpipe_trap();
    }
}
```