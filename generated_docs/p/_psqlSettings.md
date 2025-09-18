# _psqlSettings

## Location
[src/bin/psql/settings.h:80-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/settings.h#L80-L156)

## Overview
The  structure is the central configuration and state management structure for the PostgreSQL interactive terminal (psql), containing all global settings, connection information, and runtime state.

## Definition


## Detailed Description
The  structure serves as the global configuration repository for psql, PostgreSQL's interactive terminal interface. This structure maintains all runtime state, user preferences, connection details, and command-line behavior settings. It acts as a centralized hub that various psql components reference to determine how to behave in different contexts.

The structure is divided into several logical groups: database connection management, output formatting and redirection, query execution options, user interface behavior, command history and prompting, error handling, and variable storage. Many of the boolean and enumerated fields at the end of the structure are automatically updated by assignment hooks when corresponding shell variables are modified.

## Parameters / Member Variables
- : Active PostgreSQL database connection handle
- : Current client character encoding setting
- : File stream for query result output (stdout by default)
- : Flag indicating if queryFout was opened via popen()
- : File stream used for \copy command operations
- : Most recently encountered error result for reference
- : Current print formatting options (alignment, borders, etc.)
- : Temporary filename for \g command output redirection  
- : Saved print options when using \g command
- : Variable name prefix for \gset command
- : One-time flag to describe query results instead of executing
- : One-time flag to execute query results as SQL commands
- : One-time flag to use extended query protocol with parameters
- : Number of bind parameters for extended query protocol
- : Array of parameter values for extended query protocol
- : One-time flag to format results as crosstab
- : Arguments array for \crosstabview command
- : Flag indicating non-interactive terminal usage
- : Controls password prompting behavior (trivalue enum)
- : File handle of current command input source
- : Flag indicating if current command source is interactive
- : Backend PostgreSQL server version number
- : Program name (typically "psql")
- : Path to currently processed input file, if any
- : Current line number for error reporting
- : Line number within current SQL statement
- : Flag to enable query execution timing display
- : Handle for session logging file
- : Repository for user-defined shell variables
- : Stashed unusable connection for potential reconnection
- : Automatic transaction commit behavior
- : Stop execution on SQL errors
- : Suppress informational messages
- : Single-line input mode flag
- : Step through commands one at a time
- : Hide compression information in \d+ commands
- : Hide table access method information
- : Number of rows to fetch at once
- : Maximum command history size
- : Number of EOF characters to ignore
- : Command echoing mode setting
- : Hidden command echoing mode
- : Rollback behavior on errors
- : Case sensitivity for tab completion
- : Command history control settings
- , , : Command prompt strings
- : Error message verbosity level
- : Display all result sets from multi-statement queries

## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](printQueryOpt.md) (print formatting options structure)
  - [trivalue](../t/trivalue.md) (three-state enumeration type)
  - VariableSpace (shell variable storage system)
  - PSQL_ECHO (command echo enumeration)
  - PSQL_ECHO_HIDDEN (hidden command echo enumeration) 
  - PSQL_ERROR_ROLLBACK (error rollback behavior enumeration)
  - PSQL_COMP_CASE (completion case enumeration)
  - [HistControl](../H/HistControl.md) (history control enumeration)
  - PGVerbosity (PostgreSQL error verbosity enumeration)
  - PGContextVisibility (error context visibility enumeration)
- Called from (representative examples):
  - No direct references found (used as singleton global instance)

## Notes and Other Information
This structure is typically instantiated as a single global variable named  in psql's main module. The structure's design reflects psql's architecture where most configuration is handled through shell variables that trigger assignment hooks to update the corresponding fields in this structure. The separation between "user-settable" fields (those controlled by shell variables) and "internal" fields (managed directly by psql code) provides a clean interface for configuration management while maintaining internal state consistency.