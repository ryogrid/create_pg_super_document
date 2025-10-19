# pg_logging_init

## Location
[src/common/logging.c:83-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/logging.c#L83-L162)

## Overview
Initializes PostgreSQL's logging system by setting up color support, terminal detection, program name, and default log level for client utilities and applications.

## Definition
void pg_logging_init(const char *argv0)

## Detailed Description
This function performs essential initialization of PostgreSQL's logging subsystem and should be called before any output happens. It handles several key responsibilities:

1. **Color Terminal Detection**: Determines if the current terminal supports colored output by checking if stderr is a TTY
2. **Windows VT100 Support**: On Windows platforms, enables VT100 sequence processing for ANSI color support
3. **Environment Variable Processing**: 
   - Reads PG_COLOR environment variable to control color output (always/auto/never)
   - Processes PG_COLORS environment variable to customize color schemes for different message types
4. **Buffer Configuration**: Sets stderr to unbuffered mode for immediate output
5. **Program Name Setup**: Extracts and stores the program name from argv0
6. **Default Log Level**: Sets the initial logging level to PG_LOG_INFO

The color customization system supports four message types: error, warning, note, and locus, each with configurable SGR (Select Graphic Rendition) codes.

## Parameters / Member Variables
- : The first argument from the command line (typically the program path), used to extract the program name for logging purposes

## Dependencies
- Functions called/Symbols referenced:
  - [enable_vt_processing](../e/enable_vt_processing.md) (Windows only)
  - [get_progname](../g/get_progname.md)
  - getenv
  - isatty
  - fileno
  - setvbuf
  - strcmp
  - strdup
  - strtok
  - strchr
  - free
  - PG_LOG_INFO
  - SGR_ERROR_DEFAULT
  - SGR_WARNING_DEFAULT
  - SGR_NOTE_DEFAULT
  - SGR_LOCUS_DEFAULT
- Called from (representative examples):
  - [main](../m/main.md) functions in various PostgreSQL client utilities (pg_dump, psql, initdb, etc.)

## Notes and Other Information
- Must be called before any logging output occurs
- Color support depends on terminal capabilities and environment variables
- On Windows, requires VT100-compatible terminal for color output
- The PG_COLORS environment variable uses colon-separated name=value pairs (e.g., 'error=01;31:warning=01;33')
- Sets global variables for program name, log level, and color codes
- Memory allocated for color strings is not freed (intentionally, as they persist for program lifetime)
- Default color codes are used if PG_COLORS is not specified but colors are enabled

## Simplified Source

```c
void pg_logging_init(const char *argv0)
{
    const char *pg_color_env = getenv("PG_COLOR");
    bool log_color = false;
    bool color_terminal = isatty(fileno(stderr));

#ifdef WIN32
    // Enable VT100 processing on Windows if terminal supports it
    if (color_terminal)
        color_terminal = enable_vt_processing();
#endif

    // Set stderr to unbuffered for immediate output
    setvbuf(stderr, NULL, _IONBF, 0);

    // Initialize basic logging settings
    progname = get_progname(argv0);
    __pg_log_level = PG_LOG_INFO;

    // Determine if colors should be enabled
    if (pg_color_env) {
        if (strcmp(pg_color_env, "always") == 0 ||
            (strcmp(pg_color_env, "auto") == 0 && color_terminal))
            log_color = true;
    }

    // Set up color codes if colors are enabled
    if (log_color) {
        const char *pg_colors_env = getenv("PG_COLORS");

        if (pg_colors_env) {
            // Parse custom color settings from PG_COLORS
            char *colors = strdup(pg_colors_env);
            if (colors) {
                // Process colon-separated name=value pairs
                for (char *token = strtok(colors, ":"); token; token = strtok(NULL, ":")) {
                    char *separator = strchr(token, '=');
                    if (separator) {
                        *separator = '\0';
                        char *name = token;
                        char *value = separator + 1;

                        // Set color codes for different message types
                        if (strcmp(name, "error") == 0)
                            sgr_error = strdup(value);
                        if (strcmp(name, "warning") == 0)
                            sgr_warning = strdup(value);
                        if (strcmp(name, "note") == 0)
                            sgr_note = strdup(value);
                        if (strcmp(name, "locus") == 0)
                            sgr_locus = strdup(value);
                    }
                }
                free(colors);
            }
        } else {
            // Use default color codes
            sgr_error = SGR_ERROR_DEFAULT;
            sgr_warning = SGR_WARNING_DEFAULT;
            sgr_note = SGR_NOTE_DEFAULT;
            sgr_locus = SGR_LOCUS_DEFAULT;
        }
    }
}
```