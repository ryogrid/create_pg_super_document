# VacObjFilter

## Location
[src/bin/scripts/vacuumdb.c:61-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/vacuumdb.c#L61-L91)

## Overview
VacObjFilter is an enumeration that defines bitwise flags used by the vacuumdb utility to track which command-line object filtering options have been specified by the user.

## Definition

```c
OBJFILTER_SCHEMA_EXCLUDE = (1 << 4),	/* -N | --exclude-schema */
} VacObjFilter;

VacObjFilter objfilter = OBJFILTER_NONE;

static void vacuum_one_database(ConnParams *cparams,
								vacuumingOptions *vacopts,
								int stage,
								SimpleStringList *objects,
								int concurrentCons,
								const char *progname, bool echo, bool quiet);

static void vacuum_all_databases(ConnParams *cparams,
								 vacuumingOptions *vacopts,
								 bool analyze_in_stages,
								 SimpleStringList *objects,
								 int concurrentCons,
								 const char *progname, bool echo, bool quiet);

static void prepare_vacuum_command(PQExpBuffer sql, int serverVersion,
								   vacuumingOptions *vacopts, const char *table);

static void run_vacuum_command(PGconn *conn, const char *sql, bool echo,
							   const char *table);

static void help(const char *progname);

void		check_objfilter(void);

static char *escape_quotes(const char *src);

/* For analyze-in-stages mode */
#define ANALYZE_NO_STAGE	-1
#define ANALYZE_NUM_STAGES	3


int
main(int argc, char *argv[])
```
## Detailed Description
VacObjFilter is a bitwise enumeration that enables the vacuumdb command-line utility to track which object filtering options the user has specified. The enum values are designed as bit flags that can be combined using bitwise OR operations to represent multiple filter types simultaneously. This allows the program to detect incompatible option combinations and validate command-line arguments before proceeding with vacuum operations.

The global variable `objfilter` of type VacObjFilter is used throughout the vacuumdb.c file to accumulate these flags as command-line options are parsed. The filter state is then validated using the `check_objfilter()` function to ensure mutually exclusive options are not used together.

## Parameters / Member Variables
- `OBJFILTER_NONE`: Default state with no filtering options specified
- `OBJFILTER_ALL_DBS`: Set when `-a` or `--all` option is used to vacuum all databases
- `OBJFILTER_DATABASE`: Set when `-d` or `--dbname` option is used to specify a particular database
- `OBJFILTER_TABLE`: Set when `-t` or `--table` option is used to specify particular tables
- `OBJFILTER_SCHEMA`: Set when `-n` or `--schema` option is used to vacuum tables in specific schemas
- `OBJFILTER_SCHEMA_EXCLUDE`: Set when `-N` or `--exclude-schema` option is used to exclude specific schemas

## Dependencies
- Functions called/Symbols referenced:
  - Used in conjunction with `check_objfilter` function for validation
  - Referenced in command-line option parsing in `main` function
- Called from (representative examples):
  - [main](../m/main.md) function during option parsing (lines 173, 176, 197, 201, 216, 291)
  - [check_objfilter](../c/check_objfilter.md) function for validation

## Notes and Other Information
- The enum is defined as a bitwise flag system, allowing multiple filters to be combined using the OR operator (|)
- A global variable `objfilter` is declared and initialized to `OBJFILTER_NONE`
- The `check_objfilter()` function validates that incompatible combinations are not used:
  - Cannot vacuum all databases and a specific database simultaneously
  - Cannot vacuum tables by schema and specific tables simultaneously  
  - Cannot vacuum specific tables while excluding schemas
  - Cannot include and exclude schemas simultaneously
- Located in src/bin/scripts/vacuumdb.c, this is part of the PostgreSQL client-side utilities
- Each flag corresponds directly to a specific command-line option for the vacuumdb utility