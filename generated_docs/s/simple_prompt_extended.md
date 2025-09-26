# simple_prompt_extended

## Location
src/common/sprompt.c: 53 - 181

## Overview
An extended version of interactive user input function that supports cancellation via SIGINT signal handling through a provided interrupt context.

## Definition
```c
char *simple_prompt_extended(const char *prompt, bool echo, PromptInterruptContext *prompt_ctx)
```

## Detailed Description
The `simple_prompt_extended` function provides the core implementation for interactive user input in PostgreSQL client tools. It extends the basic prompting functionality with support for signal-based cancellation through the `PromptInterruptContext` parameter.

The function handles cross-platform terminal I/O differences automatically:
- On Unix-like systems, it attempts to open `/dev/tty` for both input and output to ensure direct terminal access
- On Windows, it uses `CONIN$` and `CONOUT$` console handles with proper code page handling
- Falls back to `stdin`/`stderr` if direct terminal access fails

For password input (when `echo` is false), the function temporarily disables terminal echo using platform-specific methods:
- Unix: Uses `termios.h` functions (`tcgetattr`/`tcsetattr`) to control the `ECHO` flag
- Windows: Uses `SetConsoleMode()` to configure console input mode

The function includes special handling for Windows console code page issues and MSYS environment detection.

## Parameters / Member Variables
- `prompt`: The text prompt to display to the user, or NULL if no prompt is needed (automatically localized using gettext)
- `echo`: Boolean flag controlling input visibility - false for password input (characters hidden), true to display typed characters
- `prompt_ctx`: Optional interrupt context allowing cancellation via existing SIGINT handler that can longjmp when `*(prompt_ctx->enabled)` is true

## Dependencies
- Functions called/Symbols referenced:
  - PromptInterruptContext (struct type)
  - fopen
  - pg_get_line
  - pg_strip_crlf
  - pg_strdup
  - tcgetattr/tcsetattr (Unix)
  - GetConsoleMode/SetConsoleMode (Windows)
- Called from (representative examples):
  - simple_prompt (src/common/sprompt.c:40)
  - exec_command_password (src/bin/psql/command.c:2159, 2161)
  - exec_command_prompt (src/bin/psql/command.c:2241)
  - prompt_for_password (src/bin/psql/command.c:3346, 3352)

## Notes and Other Information
- The returned string is malloc'd and caller is responsible for freeing it
- Automatically strips trailing newlines and carriage returns from input
- When canceled via interrupt context, returns an empty string and sets `prompt_ctx->canceled` to true
- Handles platform-specific terminal echo control for secure password entry
- Includes workarounds for Windows console code page issues and MSYS environment limitations
- Restores original terminal settings after input completion
- Echoes a newline after password input or cancellation to maintain proper terminal formatting
- Used as the underlying implementation for all interactive prompts in PostgreSQL client tools