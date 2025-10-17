# save_ps_display_args

## Location
[src/backend/utils/misc/ps_status.c:117-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/ps_status.c#L117-L266)

## Overview
Saves the original argc/argv values during early startup and prepares the process for subsequent ps_display modifications by preserving command-line arguments and potentially relocating environment strings.

## Definition

```c
char	  **
save_ps_display_args(int argc, char **argv)
```
## Detailed Description
This function is called early in PostgreSQL startup to preserve the original command-line arguments before they may be modified for process status display purposes. On platforms that support argv clobbering (PS_USE_CLOBBER_ARGV), it calculates the available space in the argv area and relocates environment strings to make room for longer status displays.

The function implements platform-specific handling:
- Creates copies of argv[] and environ[] arrays when clobbering is enabled
- Handles special cases like musl libc's static LD_LIBRARY_PATH pointer on Linux
- Updates macOS's static argv pointer via _NSGetArgv()
- Calculates the total buffer size available for process status display

The preserved arguments allow PostgreSQL processes to show meaningful status information while maintaining the ability to parse command-line options correctly.

## Parameters / Member Variables
- `argc`: The number of command-line arguments passed to the program
- `**argv`: Array of command-line argument strings
## Dependencies
- Functions called/Symbols referenced:
  - malloc (memory allocation)
  - [write_stderr](../w/write_stderr.md) (error output)
  - strdup (string duplication)
  - _NSGetArgv (macOS-specific argv pointer access)
- Called from (representative examples):
  - [main](../m/main.md) (in src/backend/main/main.c:91)

## Notes and Other Information
- Must be called before any code that might rely on getenv() results, as environment strings may be relocated
- Cannot use elog() for error reporting as logging is not yet initialized during early startup
- The PS_USE_CLOBBER_ARGV compilation flag determines whether argv clobbering optimization is available
- Special handling for musl libc on Linux prevents overwriting LD_LIBRARY_PATH values that the dynamic linker references
- Returns a potentially modified argv pointer that should be used for subsequent argument parsing

## Simplified Source

```c
char **save_ps_display_args(int argc, char **argv) {
    // Save original argc/argv for later use
    save_argc = argc;
    save_argv = argv;

#if defined(PS_USE_CLOBBER_ARGV)
    {
        char *end_of_area = NULL;
        char **new_environ;
        int i;

        // Find contiguous argv strings to determine available space
        for (i = 0; i < argc; i++) {
            if (i == 0 || end_of_area + 1 == argv[i])
                end_of_area = argv[i] + strlen(argv[i]);
        }

        if (end_of_area == NULL) {
            ps_buffer = NULL;
            ps_buffer_size = 0;
            return argv;
        }

        // Find contiguous environ strings following argv
        for (i = 0; environ[i] != NULL; i++) {
            if (end_of_area + 1 == environ[i]) {
#if defined(__linux__) && (!defined(__GLIBC__) && !defined(__UCLIBC__))
                // Special handling for musl libc LD_LIBRARY_PATH
                if (strncmp(environ[i], "LD_LIBRARY_PATH=", 16) == 0) {
                    end_of_area = environ[i] + 15;  // Stop at equals sign
                } else
#endif
                {
                    end_of_area = environ[i] + strlen(environ[i]);
                }
            }
        }

        // Set up process status buffer
        ps_buffer = argv[0];
        last_status_len = ps_buffer_size = end_of_area - argv[0];

        // Create new environment array to preserve original strings
        new_environ = malloc((i + 1) * sizeof(char *));
        if (!new_environ) {
            write_stderr("out of memory\n");
            exit(1);
        }

        for (i = 0; environ[i] != NULL; i++) {
            new_environ[i] = strdup(environ[i]);
            if (!new_environ[i]) {
                write_stderr("out of memory\n");
                exit(1);
            }
        }
        new_environ[i] = NULL;
        environ = new_environ;

        // Create new argv array to preserve original arguments
        char **new_argv = malloc((argc + 1) * sizeof(char *));
        if (!new_argv) {
            write_stderr("out of memory\n");
            exit(1);
        }

        for (i = 0; i < argc; i++) {
            new_argv[i] = strdup(argv[i]);
            if (!new_argv[i]) {
                write_stderr("out of memory\n");
                exit(1);
            }
        }
        new_argv[argc] = NULL;

#if defined(__darwin__)
        // Update macOS static argv pointer
        *_NSGetArgv() = new_argv;
#endif

        argv = new_argv;
    }
#endif

    return argv;
}
```