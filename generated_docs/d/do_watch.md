# do_watch

## Location
[src/bin/psql/command.c:5333-5573](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L5333-L5573)

## Overview
Implements the PostgreSQL psql \watch command functionality, repeatedly executing a query at specified intervals with optional iteration limits and minimum row constraints.

## Definition

```c
static bool do_watch(PQExpBuffer query_buf, double sleep, int iter, int min_rows)
```
## Detailed Description
The  function provides the core implementation for psql's \watch command, which repeatedly executes a SQL query at regular intervals. It handles cross-platform timing mechanisms, signal management for graceful interruption, and optional pager integration for output display.

The function sets up interval timers (Unix) or uses pg_usleep loops (Windows) to control execution timing. On Unix systems, it uses signal handling (SIGALRM, SIGINT, SIGCHLD) for precise timing and clean interruption. It supports optional pager integration via PSQL_WATCH_PAGER environment variable and includes sophisticated title generation with timestamps for each execution.

The implementation includes robust error handling, iteration counting, minimum row filtering, and proper cleanup of resources including timers, signal masks, and pager processes.

## Parameters / Member Variables
- `query_buf`: PQExpBuffer containing the SQL query to execute repeatedly
- `sleep`: Time interval between executions in seconds (converted to milliseconds internally)
- `iter`: Maximum number of iterations to perform (0 = infinite)
- `min_rows`: Minimum number of rows required to continue execution

## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](../p/printQueryOpt.md) (query output formatting options)
  - sigset_t, sigemptyset, sigaddset, sigprocmask (Unix signal management)
  - [setitimer](../s/setitimer.md), ITIMER_REAL (Unix interval timer)
  - [disable_sigpipe_trap](disable_sigpipe_trap.md), restore_sigpipe_trap (signal handling utilities)
  - popen, pclose (pager process management)
  - [PSQLexecWatch](../P/PSQLexecWatch.md) (executes the query with watch-specific handling)
  - [pg_usleep](../p/pg_usleep.md) (Windows sleep implementation)
  - [pg_malloc](../p/pg_malloc.md), pg_free (PostgreSQL memory management)
- Called from (representative examples):
  - [exec_command_watch](../e/exec_command_watch.md) (handles \watch command parsing and delegation)

## Notes and Other Information
- Uses different timing strategies: Unix systems use SIGALRM with setitimer, Windows uses pg_usleep loops
- Supports PSQL_WATCH_PAGER environment variable for custom pager integration
- Includes comprehensive signal handling for SIGINT (Ctrl+C), SIGCHLD (pager exit), and SIGALRM (timer)
- Generates timestamped titles for each execution showing current time and interval
- Handles pager errors gracefully and restores terminal state appropriately
- Implements iteration counting and minimum row filtering for conditional execution
- Cross-platform compatible with platform-specific optimizations for timing and signal handling

## Simplified Source

```c
static bool do_watch(PQExpBuffer query_buf, double sleep, int iter, int min_rows) {
    long sleep_ms = (long) (sleep * 1000);
    printQueryOpt myopt = pset.popt;
    const char *strftime_fmt = "%c";
    const char *user_title = myopt.title;
    char *title;
    const char *pagerprog = NULL;
    FILE *pagerpipe = NULL;
    int title_len;
    int res = 0;
    bool done = false;

    // Validate query buffer
    if (!query_buf || query_buf->len <= 0) {
        pg_log_error("\\watch cannot be used with an empty query");
        return false;
    }

    // Set up signal handling and timer (Unix only)
    #ifndef WIN32
    sigset_t sigalrm_sigchld_sigint, sigalrm_sigchld, sigint;
    struct itimerval interval;

    sigemptyset(&sigalrm_sigchld_sigint);
    sigaddset(&sigalrm_sigchld_sigint, SIGCHLD);
    sigaddset(&sigalrm_sigchld_sigint, SIGALRM);
    sigaddset(&sigalrm_sigchld_sigint, SIGINT);

    sigprocmask(SIG_BLOCK, &sigalrm_sigchld, NULL);

    // Set up interval timer
    interval.it_value.tv_sec = sleep_ms / 1000;
    interval.it_value.tv_usec = (sleep_ms % 1000) * 1000;
    interval.it_interval = interval.it_value;
    if (setitimer(ITIMER_REAL, &interval, NULL) < 0) {
        pg_log_error("could not set timer: %m");
        done = true;
    }
    #endif

    // Set up pager if requested
    #ifndef WIN32
    pagerprog = getenv("PSQL_WATCH_PAGER");
    if (pagerprog && strspn(pagerprog, " \t\r\n") == strlen(pagerprog))
        pagerprog = NULL;
    #endif

    if (pagerprog && myopt.topt.pager &&
        isatty(fileno(stdin)) && isatty(fileno(stdout))) {
        fflush(NULL);
        disable_sigpipe_trap();
        pagerpipe = popen(pagerprog, "w");
        if (!pagerpipe)
            restore_sigpipe_trap();
    }

    // Configure output options
    if (!pagerpipe)
        myopt.topt.pager = 0;

    // Allocate title buffer
    title_len = (user_title ? strlen(user_title) : 0) + 256;
    title = pg_malloc(title_len);

    // Main execution loop
    while (!done) {
        time_t timer;
        char timebuf[128];

        // Generate timestamped title
        timer = time(NULL);
        strftime(timebuf, sizeof(timebuf), strftime_fmt, localtime(&timer));

        if (user_title)
            snprintf(title, title_len, _("%s\t%s (every %gs)\n"),
                    user_title, timebuf, sleep_ms / 1000.0);
        else
            snprintf(title, title_len, _("%s (every %gs)\n"),
                    timebuf, sleep_ms / 1000.0);
        myopt.title = title;

        // Execute query
        res = PSQLexecWatch(query_buf->data, &myopt, pagerpipe, min_rows);
        if (res <= 0)
            break;

        // Check iteration count
        if (iter && (--iter <= 0))
            break;

        // Check pager status
        if (pagerpipe && ferror(pagerpipe))
            break;

        // Handle sleep timing
        if (sleep_ms == 0)
            continue;

        #ifdef WIN32
        // Windows: break sleep into short intervals
        for (long i = sleep_ms; i > 0;) {
            long s = Min(i, 1000L);
            pg_usleep(s * 1000L);
            if (cancel_pressed) {
                done = true;
                break;
            }
            i -= s;
        }
        #else
        // Unix: use signal-based waiting
        sigprocmask(SIG_BLOCK, &sigint, NULL);
        if (cancel_pressed)
            done = true;

        while (!done) {
            int signal_received;
            errno = sigwait(&sigalrm_sigchld_sigint, &signal_received);
            if (errno == EINTR)
                continue;
            if (errno != 0) {
                pg_log_error("could not wait for signals: %m");
                done = true;
                break;
            }

            // Handle received signals
            if (signal_received == SIGINT || signal_received == SIGCHLD)
                done = true;
            break;  // SIGALRM - time for next execution
        }
        sigprocmask(SIG_UNBLOCK, &sigint, NULL);
        #endif
    }

    // Cleanup
    if (pagerpipe) {
        pclose(pagerpipe);
        restore_sigpipe_trap();
    } else {
        fprintf(stdout, "\n");
        fflush(stdout);
    }

    #ifndef WIN32
    // Disable timer and restore signals
    memset(&interval, 0, sizeof(interval));
    setitimer(ITIMER_REAL, &interval, NULL);
    sigprocmask(SIG_UNBLOCK, &sigalrm_sigchld_sigint, NULL);
    #endif

    pg_free(title);
    return (res >= 0);
}
```