# PageOutput

## Location
[src/fe_utils/print.c:3089-3140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/print.c#L3089-L3140)

## Overview
Determines if a pager is needed for output and returns the appropriate FILE pointer, either for a pager process or stdout.

## Definition
```c
FILE *PageOutput(int lines, const printTableOpt *topt)
```

## Detailed Description
This function evaluates whether output should be sent through a pager program based on the number of lines to display and the print table options. It checks if the terminal supports paging (both stdin and stdout are TTYs), queries the terminal window size using ioctl/TIOCGWINSZ, and compares the output length against screen dimensions. If paging is appropriate, it launches the configured pager program (checking PSQL_PAGER, then PAGER environment variables, falling back to DEFAULT_PAGER) and returns a pipe to it. Otherwise, it returns stdout for direct output.

## Parameters / Member Variables
- `lines`: The number of lines that will be output
- `topt`: Pointer to printTableOpt structure containing paging configuration options, or NULL to disable paging

## Dependencies
- Functions called/Symbols referenced:
  - [printTableOpt](../p/printTableOpt.md) (structure type)
  - [winsize](../w/winsize.md) (system structure for terminal dimensions)  
  - DEFAULT_PAGER (fallback pager program)
  - [disable_sigpipe_trap](../d/disable_sigpipe_trap.md) (disable SIGPIPE signal handling)
  - popen (open pipe to pager process)
  - [restore_sigpipe_trap](../r/restore_sigpipe_trap.md) (restore SIGPIPE signal handling)
- Called from (representative examples):
  - [exec_command_sf_sv](../e/exec_command_sf_sv.md) (src/bin/psql/command.c:2567)
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (src/bin/psql/common.c:1668)
  - [usage](../u/usage.md), slashUsage, helpVariables (various help functions)
  - [print_aligned_text](../p/print_aligned_text.md) (src/fe_utils/print.c:888)
  - [IsPagerNeeded](../I/IsPagerNeeded.md) (src/fe_utils/print.c:3427)

## Notes and Other Information
- Uses TIOCGWINSZ ioctl to get terminal window size when available
- Handles pager program selection through environment variable hierarchy: PSQL_PAGER → PAGER → DEFAULT_PAGER
- Ignores pager if PAGER environment variable is empty or contains only whitespace
- Manages SIGPIPE signal handling around pager process creation to handle broken pipe scenarios gracefully
- Falls back to stdout silently if pager program fails to start
- Requires both stdin and stdout to be TTYs for pager to be used

## Simplified Source

```c
FILE *
PageOutput(int lines, const printTableOpt *topt)
{
    // Check if pager is needed and available
    if (topt && topt->pager && isatty(fileno(stdin)) && isatty(fileno(stdout))) {
#ifdef TIOCGWINSZ
        // Get terminal window size to determine if paging is needed
        struct winsize screen_size;
        int result = ioctl(fileno(stdout), TIOCGWINSZ, &screen_size);

        // Use pager if:
        // - Can't get screen size, OR
        // - Lines exceed screen height and minimum threshold, OR
        // - Forced pager mode (pager > 1)
        if (result == -1 ||
            (lines >= screen_size.ws_row && lines >= topt->pager_min_lines) ||
            topt->pager > 1)
#endif
        {
            // Determine pager program: PSQL_PAGER -> PAGER -> DEFAULT_PAGER
            const char *pagerprog = getenv("PSQL_PAGER");
            if (!pagerprog)
                pagerprog = getenv("PAGER");
            if (!pagerprog)
                pagerprog = DEFAULT_PAGER;
            else {
                // Skip pager if PAGER is empty or whitespace-only
                if (strspn(pagerprog, " \t\r\n") == strlen(pagerprog))
                    return stdout;
            }

            // Launch pager process
            fflush(NULL);
            disable_sigpipe_trap();
            FILE *pagerpipe = popen(pagerprog, "w");

            if (pagerpipe)
                return pagerpipe;

            // If pager fails, restore signals and fall back to stdout
            restore_sigpipe_trap();
        }
    }

    return stdout;
}
```