# initializeInput

## Location
[src/bin/psql/input.c:344-412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/input.c#L344-L412)

## Overview
Initializes the input handling subsystem for psql, setting up readline functionality, history management, and loading previous command history from files.

## Definition

```c
void
initializeInput(int flags)
```
## Detailed Description
The  function is responsible for setting up all input-related functionality in psql, particularly readline support and command history management. When called with appropriate flags, it configures the GNU Readline library for interactive command editing, loads command history from persistent storage, and sets up cleanup handlers.

The function performs several key initialization tasks:
1. Configures readline with psql-specific settings (such as SQL comment markers)
2. Determines the appropriate history file location using HISTFILE variable, PSQL_HISTORY environment variable, or default location
3. Loads existing command history from the determined file
4. Decodes any encoded newlines in the loaded history entries
5. Registers cleanup functions to be called on program exit

The readline integration provides users with command editing capabilities, including cursor movement, command completion, and access to command history through keyboard shortcuts.

## Parameters / Member Variables
- `flags`: Integer flag controlling initialization behavior. Currently, bit 0 (flags & 1) enables readline and history functionality.
## Dependencies
- Functions called/Symbols referenced:
  - [initialize_readline](initialize_readline.md) (sets up readline global variables)
  - rl_variable_bind (configures readline variables like comment-begin)
  - rl_initialize (reads ~/.inputrc configuration)
  - using_history (initializes history functionality)
  - [GetVariable](../G/GetVariable.md) (retrieves psql variables like HISTFILE)
  - getenv (retrieves environment variables like PSQL_HISTORY)
  - [get_home_path](../g/get_home_path.md) (determines user's home directory)
  - [psprintf](../p/psprintf.md) (formatted string creation)
  - [pg_strdup](../p/pg_strdup.md) (string duplication)
  - [expand_tilde](../e/expand_tilde.md) (expands ~ in file paths)
  - read_history (loads history from file)
  - [decode_history](../d/decode_history.md) (converts encoded newlines back to actual newlines)
  - atexit (registers cleanup function)
  - [finishInput](../f/finishInput.md) (cleanup function registered with atexit)
  - PSQLHISTORY (default history filename: ".psql_history")

- Called from (representative examples):
  - startup.c (main psql initialization)

## Notes and Other Information
- Only available when compiled with USE_READLINE support (requires libreadline or compatible library)
- History file location priority: HISTFILE variable > PSQL_HISTORY environment > ~/.psql_history default
- The function sets global variables useReadline and useHistory to control input behavior throughout the program
- Automatically registers finishInput() as an exit handler to ensure proper cleanup of resources
- The rl_variable_bind call sets "comment-begin" to "-- " to provide appropriate SQL comment handling in readline
- History lines are tracked via history_lines_added counter for proper management
- Tilde expansion is performed on custom history file paths to support ~/path notation

## Simplified Source

```c
void initializeInput(int flags) {
#ifdef USE_READLINE
    if (flags & 1) { // Enable readline and history
        const char *histfile;
        char home[MAXPGPATH];

        // Configure readline
        useReadline = true;
        initialize_readline();

        #ifdef HAVE_RL_VARIABLE_BIND
            // Set SQL comment marker for readline
            (void) rl_variable_bind("comment-begin", "-- ");
        #endif

        // Initialize readline (reads ~/.inputrc)
        rl_initialize();

        // Initialize history functionality
        useHistory = true;
        using_history();
        history_lines_added = 0;

        // Determine history file location
        histfile = GetVariable(pset.vars, "HISTFILE");

        // Try PSQL_HISTORY environment variable if HISTFILE not set
        if (histfile == NULL) {
            char *envhist = getenv("PSQL_HISTORY");
            if (envhist != NULL && strlen(envhist) > 0) {
                histfile = envhist;
            }
        }

        // Use default location in home directory if no explicit file
        if (histfile == NULL) {
            if (get_home_path(home)) {
                psql_history = psprintf("%s/%s", home, PSQLHISTORY);
            }
        } else {
            // Use specified file with tilde expansion
            psql_history = pg_strdup(histfile);
            expand_tilde(&psql_history);
        }

        // Load existing history from file
        if (psql_history) {
            read_history(psql_history);
            decode_history(); // Convert encoded newlines
        }
    }
#endif

    // Register cleanup function for program exit
    atexit(finishInput);
}
```