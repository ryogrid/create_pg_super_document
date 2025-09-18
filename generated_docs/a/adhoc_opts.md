# adhoc_opts

## Location
src/bin/psql/startup.c: 66 - 88

## Overview
A structure that holds all command-line options and parsed arguments for psql, serving as the primary container for startup configuration.

## Definition


## Detailed Description
The adhoc_opts structure serves as the central repository for all command-line options parsed by psql during startup. It stores connection parameters, behavioral flags, and a queue of actions to be executed. The structure is populated by parse_psql_options() which processes argc/argv using getopt_long, and is then used throughout the main() function to configure psql's behavior and execute the requested actions. This design separates option parsing from option processing, allowing for clean initialization and execution phases.

## Parameters / Member Variables
- `dbname`: Database name to connect to (from -d option or positional argument)
- `host`: Database server hostname or IP address (from -h option)
- `port`: Database server port number (from -p option)  
- `username`: Database username for authentication (from -U option or positional argument)
- `logfilename`: Path to log file for query logging (from -L option)
- `no_readline`: Disable readline/libedit support for input (from -n option)
- `no_psqlrc`: Skip reading .psqlrc configuration files (from -X option)
- `single_txn`: Execute all commands in a single transaction (from -1 option)
- `list_dbs`: List available databases and exit (from -l option)
- `actions`: Queue of commands/queries/files to execute (from -c, -f options)

## Dependencies
- Functions called/Symbols referenced:
  - SimpleActionList (for storing queued actions)
  - parse_psql_options (initializes and populates the structure)
- Called from (representative examples):
  - main (declares and uses an instance to control psql behavior)
  - parse_psql_options (receives pointer to populate fields)

## Notes and Other Information
- The structure is initialized with memset() to zero all fields before option parsing
- String fields are allocated with pg_strdup() when set from command-line arguments
- Boolean fields default to false and are set to true when their corresponding options are specified
- The actions field is processed sequentially after database connection is established
- Used only during psql startup and is not accessed after the main initialization phase
- Represents the "ad hoc" nature of command-line specified options versus configuration file settings