# write_stderr

## Location
[src/bin/pg_ctl/pg_ctl.c:115-201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_ctl/pg_ctl.c#L115-L201)

## Overview
A low-level error output function that writes formatted error messages to stderr or equivalent output mechanisms, designed for use during early PostgreSQL startup before the full error handling system is available.

## Definition

```c
#endif


static void write_stderr(const char *fmt,...) pg_attribute_printf(1, 2);
static void do_advice(void);
static void do_help(void);
static void set_mode(char *modeopt);
static void set_sig(char *signame);
static void do_init(void);
static void do_start(void);
static void do_stop(void);
static void do_restart(void);
static void do_reload(void);
static void do_status(void);
static void do_promote(void);
static void do_logrotate(void);
static void do_kill(pid_t pid);
static void print_msg(const char *msg);
static void adjust_data_dir(void);

#ifdef WIN32
#include <versionhelpers.h>
static bool pgwin32_IsInstalled(SC_HANDLE);
static char *pgwin32_CommandLine(bool);
static void pgwin32_doRegister(void);
static void pgwin32_doUnregister(void);
static void pgwin32_SetServiceStatus(DWORD);
static void WINAPI pgwin32_ServiceHandler(DWORD);
static void WINAPI pgwin32_ServiceMain(DWORD, LPTSTR *);
static void pgwin32_doRunAsService(void);
static int	CreateRestrictedProcess(char *cmd, PROCESS_INFORMATION *processInfo, bool as_service);
static PTOKEN_PRIVILEGES GetPrivilegesToDelete(HANDLE hToken);
#endif

static pid_t get_pgpid(bool is_status_request);
static char **readfile(const char *path, int *numlines);
static void free_readfile(char **optlines);
static pid_t start_postmaster(void);
static void read_post_opts(void);

static WaitPMResult wait_for_postmaster_start(pid_t pm_pid, bool do_checkpoint);
static bool wait_for_postmaster_stop(void);
static bool wait_for_postmaster_promote(void);
static bool postmaster_is_alive(pid_t pid);

#if defined(HAVE_GETRLIMIT)
static void unlimit_core_size(void);
#endif

static DBState get_control_dbstate(void);


#ifdef WIN32
static void
write_eventlog(int level, const char *line)
```
## Detailed Description
The  function provides a basic error output mechanism that can be used safely before PostgreSQL's full error reporting system (ereport/elog) is initialized. It handles platform-specific differences between Unix and Windows systems for error output.

On Unix systems, it directly uses  to write to stderr. On Windows, it determines whether PostgreSQL is running as a service or console application - if running as a service, it writes to the Windows event log using , otherwise it writes to the console using .

The function supports variable arguments like printf-style formatting and automatically applies internationalization translation to the format string using the  macro.

## Parameters / Member Variables
- : Format string (printf-style) that will be internationalized via 
- : Variable arguments corresponding to format specifiers in the format string

## Dependencies
- Functions called/Symbols referenced:
  -  (Unix path)
  -  (Windows path)
  -  (Windows service detection)
  -  (Windows service logging)
  -  (Windows console output)
- Called from (representative examples):
  -  (bootstrap.c:282, 291)
  -  (postmaster.c:661, 734, 746, etc.)
  -  (assert.c:37, 40)
  -  (pg_ctl.c:254, 257, 270, etc.)
  -  (guc.c:1802, 1806, 1830, etc.)

## Notes and Other Information
- Used extensively throughout PostgreSQL for early startup error reporting
- Critical for debugging issues that occur before the main error handling system is available
- Automatically handles platform differences between Unix and Windows
- Format strings are automatically translated for internationalization
- Essential for pg_ctl utility error reporting
- Buffer size on Windows is arbitrarily set to 2048 characters

## Simplified Source

```c
// Simplified version of write_stderr
void write_stderr(const char *fmt, ...) {
    va_list ap;

    // Apply internationalization to format string
    fmt = _(fmt);

    va_start(ap, fmt);

#ifndef WIN32
    // Unix path: Direct output to stderr
    vfprintf(stderr, fmt, ap);
    fflush(stderr);
#else
    // Windows path: Buffer the message first
    char errbuf[2048];
    vsnprintf(errbuf, sizeof(errbuf), fmt, ap);

    // Choose output method based on service vs console mode
    if (pgwin32_is_service()) {
        // Running as Windows service: write to event log
        write_eventlog(ERROR, errbuf, strlen(errbuf));
    } else {
        // Running as console app: write to console
        write_console(errbuf, strlen(errbuf));
        fflush(stderr);
    }
#endif

    va_end(ap);
}
```

Key simplifications made:
- Consolidated platform-specific conditional compilation into clear sections
- Added descriptive comments for each major code path
- Simplified variable declarations to focus on essential functionality
- Maintained the core logic: format message, choose output method, write message
- Preserved critical platform differences between Unix and Windows handling