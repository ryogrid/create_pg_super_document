# parse_psql_options

## Location
[src/bin/psql/startup.c:483-747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/startup.c#L483-L747)

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
  - [getopt_long](../g/getopt_long.md) (GNU command-line parsing)
  - [SetVariable](../S/SetVariable.md) (psql variable setting)
  - [SetVariableBool](../S/SetVariableBool.md) (boolean variable setting)
  - [simple_action_list_append](../s/simple_action_list_append.md) (action queue management)
  - [setQFout](../s/setQFout.md) (output file configuration)
  - do_pset (print setting configuration)
  - [DeleteVariable](../D/DeleteVariable.md) (variable removal)
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

## Simplified Source

```c
static void parse_psql_options(int argc, char *argv[], struct adhoc_opts *options) {
    // Define all the command-line options
    static struct option long_options[] = {
        {"echo-all", no_argument, NULL, 'a'},
        {"no-align", no_argument, NULL, 'A'},
        {"command", required_argument, NULL, 'c'},
        {"dbname", required_argument, NULL, 'd'},
        {"echo-queries", no_argument, NULL, 'e'},
        {"file", required_argument, NULL, 'f'},
        {"host", required_argument, NULL, 'h'},
        {"list", no_argument, NULL, 'l'},
        {"port", required_argument, NULL, 'p'},
        {"username", required_argument, NULL, 'U'},
        {"version", no_argument, NULL, 'V'},
        {"help", optional_argument, NULL, 1},
        // ... other options
        {NULL, 0, NULL, 0}
    };

    memset(options, 0, sizeof(*options));

    int c;
    while ((c = getopt_long(argc, argv, "aAbc:d:eEf:F:h:HlL:no:p:P:qR:sStT:U:v:VwWxXz?01",
                           long_options, NULL)) != -1) {
        switch (c) {
            case 'a':  // --echo-all
                SetVariable(pset.vars, "ECHO", "all");
                break;
            case 'A':  // --no-align
                pset.popt.topt.format = PRINT_UNALIGNED;
                break;
            case 'c':  // --command
                if (optarg[0] == '\\')
                    simple_action_list_append(&options->actions, ACT_SINGLE_SLASH, optarg + 1);
                else
                    simple_action_list_append(&options->actions, ACT_SINGLE_QUERY, optarg);
                break;
            case 'd':  // --dbname
                options->dbname = pg_strdup(optarg);
                break;
            case 'e':  // --echo-queries
                SetVariable(pset.vars, "ECHO", "queries");
                break;
            case 'f':  // --file
                simple_action_list_append(&options->actions, ACT_FILE, optarg);
                break;
            case 'h':  // --host
                options->host = pg_strdup(optarg);
                break;
            case 'H':  // --html
                pset.popt.topt.format = PRINT_HTML;
                break;
            case 'l':  // --list
                options->list_dbs = true;
                break;
            case 'p':  // --port
                options->port = pg_strdup(optarg);
                break;
            case 'P':  // --pset (complex option with = syntax)
                char *value = pg_strdup(optarg);
                char *equal_loc = strchr(value, '=');
                if (!equal_loc)
                    do_pset(value, NULL, &pset.popt, true);
                else {
                    *equal_loc = '\0';
                    do_pset(value, equal_loc + 1, &pset.popt, true);
                }
                free(value);
                break;
            case 'q':  // --quiet
                SetVariableBool(pset.vars, "QUIET");
                break;
            case 'U':  // --username
                options->username = pg_strdup(optarg);
                break;
            case 'v':  // --variable (complex option with = syntax)
                char *var_value = pg_strdup(optarg);
                char *var_equal = strchr(var_value, '=');
                if (!var_equal)
                    DeleteVariable(pset.vars, var_value);
                else {
                    *var_equal = '\0';
                    SetVariable(pset.vars, var_value, var_equal + 1);
                }
                free(var_value);
                break;
            case 'V':  // --version
                showVersion();
                exit(EXIT_SUCCESS);
            case '?':  // help or error
                if (strcmp(argv[optind - 1], "-?") == 0) {
                    usage(NOPAGER);
                    exit(EXIT_SUCCESS);
                } else {
                    goto unknown_option;
                }
                break;
            case 1:  // --help with optional argument
                if (!optarg || strcmp(optarg, "options") == 0)
                    usage(NOPAGER);
                else if (strcmp(optarg, "commands") == 0)
                    slashUsage(NOPAGER);
                else if (strcmp(optarg, "variables") == 0)
                    helpVariables(NOPAGER);
                exit(EXIT_SUCCESS);
            default:
            unknown_option:
                pg_log_error_hint("Try \"%s --help\" for more information.", pset.progname);
                exit(EXIT_FAILURE);
        }
    }

    // Handle remaining positional arguments (dbname, username)
    while (argc - optind >= 1) {
        if (!options->dbname)
            options->dbname = argv[optind];
        else if (!options->username)
            options->username = argv[optind];
        else if (!pset.quiet)
            pg_log_warning("extra command-line argument \"%s\" ignored", argv[optind]);
        optind++;
    }
}
```