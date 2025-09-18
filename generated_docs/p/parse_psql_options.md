# parse_psql_options

## Location
src/bin/psql/startup.c: 483 - 747

## Overview
A comprehensive command-line argument parser for psql that processes all supported options and configures the psql environment accordingly.

## Definition
```c
static void parse_psql_options(int argc, char *argv[], struct adhoc_opts *options)
```

## Detailed Description
This function is responsible for parsing all command-line arguments passed to psql and configuring the application state based on those arguments. It uses the GNU getopt_long function to handle both short and long option formats, supporting a comprehensive set of psql options including database connection parameters, output formatting options, behavioral settings, and action commands.

The function processes options such as connection parameters (-h, -p, -U, -d), output formatting (-A, -H, -t, -F), behavioral controls (-q, -s, -S, -n), and special actions (-c, -f, -l). It also handles help and version requests, variable assignments, and various psql-specific settings like echo modes and readline control.

After processing all options, the function also handles positional arguments for database name and username if they were not specified via options.

## Parameters / Member Variables
- `argc`: Number of command-line arguments
- `argv`: Array of command-line argument strings  
- `options`: Pointer to adhoc_opts structure that will be populated with parsed option values

## Dependencies
- Functions called/Symbols referenced:
  - getopt_long (GNU command-line parsing)
  - SetVariable (psql variable setting)
  - SetVariableBool (boolean variable setting)
  - [simple_action_list_append](../s/simple_action_list_append.md) (action queue management)
  - [setQFout](../s/setQFout.md) (output file configuration)
  - do_pset (print setting configuration)
  - DeleteVariable (variable removal)
  - [showVersion](../s/showVersion.md) (version display)
  - [usage](../u/usage.md) (help display)
  - [slashUsage](../s/slashUsage.md) (slash command help)
  - [helpVariables](../h/helpVariables.md) (variable help)
  - [pg_strdup](pg_strdup.md) (string duplication)
  - pg_log_warning (warning logging)
  - pg_log_error_hint (error logging)
- Called from (representative examples):
  - [main](../m/main.md) (psql startup)

## Notes and Other Information
- This is a static function local to src/bin/psql/startup.c
- Supports both short options (-h) and long options (--host)
- Handles complex option parsing like -P and -v that can include = assignments
- Processes special help options with optional arguments (--help[=topic])
- Manages the global pset structure for psql configuration
- Exits the program for version/help requests or parsing errors
- Supports CSV, HTML, unaligned, and other output formats
- Handles password prompting controls (-w, -W)
- Manages single transaction mode and other execution controls