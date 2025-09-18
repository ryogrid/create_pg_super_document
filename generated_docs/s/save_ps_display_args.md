# save_ps_display_args

## Location
src/backend/utils/misc/ps_status.c: 117 - 266

## Overview
Saves the original argc/argv values during early startup and prepares the process for subsequent ps_display modifications by preserving command-line arguments and potentially relocating environment strings.

## Definition


## Detailed Description
This function is called early in PostgreSQL startup to preserve the original command-line arguments before they may be modified for process status display purposes. On platforms that support argv clobbering (PS_USE_CLOBBER_ARGV), it calculates the available space in the argv area and relocates environment strings to make room for longer status displays.

The function implements platform-specific handling:
- Creates copies of argv[] and environ[] arrays when clobbering is enabled
- Handles special cases like musl libc's static LD_LIBRARY_PATH pointer on Linux
- Updates macOS's static argv pointer via _NSGetArgv()
- Calculates the total buffer size available for process status display

The preserved arguments allow PostgreSQL processes to show meaningful status information while maintaining the ability to parse command-line options correctly.

## Parameters / Member Variables
- : The number of command-line arguments passed to the program
- : Array of command-line argument strings

## Dependencies
- Functions called/Symbols referenced:
  - malloc (memory allocation)
  - write_stderr (error output)
  - strdup (string duplication)
  - _NSGetArgv (macOS-specific argv pointer access)
- Called from (representative examples):
  - main (in src/backend/main/main.c:91)

## Notes and Other Information
- Must be called before any code that might rely on getenv() results, as environment strings may be relocated
- Cannot use elog() for error reporting as logging is not yet initialized during early startup
- The PS_USE_CLOBBER_ARGV compilation flag determines whether argv clobbering optimization is available
- Special handling for musl libc on Linux prevents overwriting LD_LIBRARY_PATH values that the dynamic linker references
- Returns a potentially modified argv pointer that should be used for subsequent argument parsing