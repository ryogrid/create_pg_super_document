# _psqlSettings

## Location
[src/bin/psql/settings.h:80-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/settings.h#L80-L156)

## Overview
The  structure is the central configuration and state management structure for the PostgreSQL interactive terminal (psql), containing all global settings, connection information, and runtime state.

## Definition

```c
typedef struct _psqlSettings
{
	PGconn	   *db;				/* connection to backend */
	int			encoding;		/* client_encoding */
	FILE	   *queryFout;		/* where to send the query results */
	bool		queryFoutPipe;	/* queryFout is from a popen() */

	FILE	   *copyStream;		/* Stream to read/write for \copy command */

	PGresult   *last_error_result;	/* most recent error result, if any */

	printQueryOpt popt;			/* The active print format settings */

	char	   *gfname;			/* one-shot file output argument for \g */
	printQueryOpt *gsavepopt;	/* if not null, saved print format settings */

	char	   *gset_prefix;	/* one-shot prefix argument for \gset */
	bool		gdesc_flag;		/* one-shot request to describe query result */
	bool		gexec_flag;		/* one-shot request to execute query result */
	bool		bind_flag;		/* one-shot request to use extended query
								 * protocol */
	int			bind_nparams;	/* number of parameters */
	char	  **bind_params;	/* parameters for extended query protocol call */
	bool		crosstab_flag;	/* one-shot request to crosstab result */
	char	   *ctv_args[4];	/* \crosstabview arguments */

	bool		notty;			/* stdin or stdout is not a tty (as determined
								 * on startup) */
	enum trivalue getPassword;	/* prompt the user for a username and password */
	FILE	   *cur_cmd_source; /* describe the status of the current main
								 * loop */
	bool		cur_cmd_interactive;
	int			sversion;		/* backend server version */
	const char *progname;		/* in case you renamed psql */
	char	   *inputfile;		/* file being currently processed, if any */
	uint64		lineno;			/* also for error reporting */
	uint64		stmt_lineno;	/* line number inside the current statement */

	bool		timing;			/* enable timing of all queries */

	FILE	   *logfile;		/* session log file handle */

	VariableSpace vars;			/* "shell variable" repository */

	/*
	 * If we get a connection failure, the now-unusable PGconn is stashed here
	 * until we can successfully reconnect.  Never attempt to do anything with
	 * this PGconn except extract parameters for a \connect attempt.
	 */
	PGconn	   *dead_conn;		/* previous connection to backend */

	/*
	 * The remaining fields are set by assign hooks associated with entries in
	 * "vars".  They should not be set directly except by those hook
	 * functions.
	 */
	bool		autocommit;
	bool		on_error_stop;
	bool		quiet;
	bool		singleline;
	bool		singlestep;
	bool		hide_compression;
	bool		hide_tableam;
	int			fetch_count;
	int			histsize;
	int			ignoreeof;
	PSQL_ECHO	echo;
	PSQL_ECHO_HIDDEN echo_hidden;
	PSQL_ERROR_ROLLBACK on_error_rollback;
	PSQL_COMP_CASE comp_case;
	HistControl histcontrol;
	const char *prompt1;
	const char *prompt2;
	const char *prompt3;
	PGVerbosity verbosity;		/* current error verbosity level */
	bool		show_all_results;
	PGContextVisibility show_context;	/* current context display level */
} PsqlSettings;
```
## Detailed Description
The  structure serves as the global configuration repository for psql, PostgreSQL's interactive terminal interface. This structure maintains all runtime state, user preferences, connection details, and command-line behavior settings. It acts as a centralized hub that various psql components reference to determine how to behave in different contexts.

The structure is divided into several logical groups: database connection management, output formatting and redirection, query execution options, user interface behavior, command history and prompting, error handling, and variable storage. Many of the boolean and enumerated fields at the end of the structure are automatically updated by assignment hooks when corresponding shell variables are modified.

## Parameters / Member Variables
- `db`: Active PostgreSQL database connection handle
- `encoding`: Current client character encoding setting
- `queryFout`: File stream for query result output (stdout by default)
- `queryFoutPipe`: Flag indicating if queryFout was opened via popen()
- `copyStream`: File stream used for \copy command operations
- `last_error_result`: Most recently encountered error result for reference
- `popt`: Current print formatting options (alignment, borders, etc.)
- `gfname`: Temporary filename for \g command output redirection  
- `gsavepopt`: Saved print options when using \g command
- `gset_prefix`: Variable name prefix for \gset command
- `gdesc_flag`: One-time flag to describe query results instead of executing
- `gexec_flag`: One-time flag to execute query results as SQL commands
- `bind_flag`: One-time flag to use extended query protocol with parameters
- `bind_nparams`: Number of bind parameters for extended query protocol
- `bind_params`: Array of parameter values for extended query protocol
- `crosstab_flag`: One-time flag to format results as crosstab
- `ctv_args`: Arguments array for \crosstabview command
- `notty`: Flag indicating non-interactive terminal usage
- `getPassword`: Controls password prompting behavior (trivalue enum)
- `cur_cmd_source`: File handle of current command input source
- `cur_cmd_interactive`: Flag indicating if current command source is interactive
- `sversion`: Backend PostgreSQL server version number
- `progname`: Program name (typically "psql")
- `inputfile`: Path to currently processed input file, if any
- `lineno`: Current line number for error reporting
- `stmt_lineno`: Line number within current SQL statement
- `timing`: Flag to enable query execution timing display
- `logfile`: Handle for session logging file
- `vars`: Repository for user-defined shell variables
- `dead_conn`: Stashed unusable connection for potential reconnection
- `autocommit`: Automatic transaction commit behavior
- `on_error_stop`: Stop execution on SQL errors
- `quiet`: Suppress informational messages
- `singleline`: Single-line input mode flag
- `singlestep`: Step through commands one at a time
- `hide_compression`: Hide compression information in \d+ commands
- `hide_tableam`: Hide table access method information
- `fetch_count`: Number of rows to fetch at once
- `histsize`: Maximum command history size
- `ignoreeof`: Number of EOF characters to ignore
- `echo`: Command echoing mode setting
- `echo_hidden`: Hidden command echoing mode
- `on_error_rollback`: Rollback behavior on errors
- `comp_case`: Case sensitivity for tab completion
- `histcontrol`: Command history control settings
- `prompt1`, `prompt2`, `prompt3`: Command prompt strings
- `verbosity`: Error message verbosity level
- `show_all_results`: Display all result sets from multi-statement queries
- `show_context`: Error context display level

## Dependencies
- Functions called/Symbols referenced:
  - [printQueryOpt](printQueryOpt.md) (print formatting options structure)
  - [trivalue](../t/trivalue.md) (three-state enumeration type)
  - [VariableSpace](../V/VariableSpace.md) (shell variable storage system)
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