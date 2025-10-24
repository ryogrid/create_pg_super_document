# simple_prompt_extended

## Location
[src/common/sprompt.c:53-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/sprompt.c#L53-L181)

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
  - [PromptInterruptContext](../P/PromptInterruptContext.md) (struct type)
  - fopen
  - [pg_get_line](../p/pg_get_line.md)
  - [pg_strip_crlf](../p/pg_strip_crlf.md)
  - [pg_strdup](../p/pg_strdup.md)
  - tcgetattr/tcsetattr (Unix)
  - GetConsoleMode/SetConsoleMode (Windows)
- Called from (representative examples):
  - [simple_prompt](simple_prompt.md) (src/common/sprompt.c:40)
  - [exec_command_password](../e/exec_command_password.md) (src/bin/psql/command.c:2159, 2161)
  - [exec_command_prompt](../e/exec_command_prompt.md) (src/bin/psql/command.c:2241)
  - [prompt_for_password](../p/prompt_for_password.md) (src/bin/psql/command.c:3346, 3352)

## Notes and Other Information
- The returned string is malloc'd and caller is responsible for freeing it
- Automatically strips trailing newlines and carriage returns from input
- When canceled via interrupt context, returns an empty string and sets `prompt_ctx->canceled` to true
- Handles platform-specific terminal echo control for secure password entry
- Includes workarounds for Windows console code page issues and MSYS environment limitations
- Restores original terminal settings after input completion
- Echoes a newline after password input or cancellation to maintain proper terminal formatting
- Used as the underlying implementation for all interactive prompts in PostgreSQL client tools

## Simplified Source

```c
char *simple_prompt_extended(const char *prompt, bool echo,
                           PromptInterruptContext *prompt_ctx) {
    FILE *termin, *termout;

    // Open terminal I/O handles (platform-specific)
#ifdef WIN32
    termin = fopen("CONIN$", "w+");
    termout = fopen("CONOUT$", "w+");
#else
    termin = fopen("/dev/tty", "r");
    termout = fopen("/dev/tty", "w");
#endif

    // Fall back to stdin/stderr if direct terminal access fails
    if (!termin || !termout) {
        if (termin) fclose(termin);
        if (termout) fclose(termout);
        termin = stdin;
        termout = stderr;
    }

    // Disable echo for password input
    if (!echo) {
#if defined(HAVE_TERMIOS_H)
        struct termios t_orig, t;
        tcgetattr(fileno(termin), &t);
        t_orig = t;
        t.c_lflag &= ~ECHO;
        tcsetattr(fileno(termin), TCSAFLUSH, &t);
#elif defined(WIN32)
        HANDLE handle = (HANDLE)_get_osfhandle(_fileno(termin));
        DWORD t_orig;
        GetConsoleMode(handle, &t_orig);
        SetConsoleMode(handle, ENABLE_LINE_INPUT | ENABLE_PROCESSED_INPUT);
#endif
    }

    // Display prompt and read input
    if (prompt) {
        fputs(_(prompt), termout);
        fflush(termout);
    }

    char *result = pg_get_line(termin, prompt_ctx);
    if (result == NULL) {
        result = pg_strdup("");
    }

    // Strip trailing newlines
    pg_strip_crlf(result);

    // Restore echo and clean up
    if (!echo) {
#if defined(HAVE_TERMIOS_H)
        tcsetattr(fileno(termin), TCSAFLUSH, &t_orig);
        fputs("\n", termout);
        fflush(termout);
#elif defined(WIN32)
        SetConsoleMode(handle, t_orig);
        fputs("\n", termout);
        fflush(termout);
#endif
    }

    if (termin != stdin) {
        fclose(termin);
        fclose(termout);
    }

    return result;
}
```