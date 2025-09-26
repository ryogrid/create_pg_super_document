# statement

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:67-94](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L67-L94)

## Overview
A comprehensive structure that represents a single SQL statement in ECPG, containing all necessary information for statement execution, parameter handling, and result management.

## Definition

```c
struct statement
{
	int			lineno;
	char	   *command;
	char	   *name;
	struct connection *connection;
	enum COMPAT_MODE compat;
	bool		force_indicator;
	enum ECPG_statement_type statement_type;
	bool		questionmarks;
	struct variable *inlist;
	struct variable *outlist;
#ifdef HAVE_USELOCALE
	locale_t	oldlocale;
#else
	char	   *oldlocale;
#ifdef HAVE__CONFIGTHREADLOCALE
	int			oldthreadlocale;
#endif
#endif
	int			nparams;
	char	  **paramvalues;
	int		   *paramlengths;
	int		   *paramformats;
	PGresult   *results;
};
```
## Detailed Description
The statement structure is the core data structure in ECPG for representing and managing SQL statements. It encapsulates all aspects of statement execution including the SQL command text, parameter binding, result handling, and execution context. This structure supports both regular SQL statements and prepared statements, with comprehensive parameter management capabilities including support for different parameter formats and lengths.

The structure handles locale-specific operations and maintains compatibility across different PostgreSQL versions through its compatibility mode settings. It manages both input parameters (for SQL execution) and output variables (for result retrieval), making it suitable for complex SQL operations that involve data binding in both directions.

## Parameters / Member Variables
- : Source line number where the statement appears in the original ECPG source code
- : The actual SQL command string to be executed
- : Optional name identifier for the statement (used for prepared statements)
- : Pointer to the database connection structure this statement belongs to
- : Compatibility mode enumeration controlling PostgreSQL version-specific behavior
- : Boolean flag controlling whether indicator variables are required
- : Enumeration specifying the type of SQL statement (SELECT, INSERT, etc.)
- : Boolean flag indicating whether the statement uses question mark parameter placeholders
- : Linked list of input variables/parameters for the statement
- : Linked list of output variables for result retrieval
- : Saved locale information for locale-sensitive operations (type varies by system)
- : Saved thread locale configuration (Windows-specific)
- : Number of parameters in the statement
- : Array of parameter values as strings
- : Array containing the length of each parameter
- : Array specifying the format (text/binary) for each parameter
- : Pointer to PostgreSQL result set from statement execution

## Dependencies
- Functions called/Symbols referenced:
  - COMPAT_MODE (enumeration for compatibility settings)
  - ECPG_statement_type (enumeration for statement types)
  - locale_t (locale type for internationalization)
- Called from (representative examples):
  - [ECPGget_desc](../E/ECPGget_desc.md) (src/interfaces/ecpg/ecpglib/descriptor.c:473)
  - [prepared_statement](../p/prepared_statement.md) (src/interfaces/ecpg/ecpglib/ecpglib_extern.h:99)
  - [free_statement](../f/free_statement.md) (src/interfaces/ecpg/ecpglib/execute.c:96)
  - [ecpg_execute](../e/ecpg_execute.md) (src/interfaces/ecpg/ecpglib/execute.c:1602)
  - [ecpg_do_prologue](../e/ecpg_do_prologue.md) (src/interfaces/ecpg/ecpglib/execute.c:1947-1971)
  - [ecpg_do_epilogue](../e/ecpg_do_epilogue.md) (src/interfaces/ecpg/ecpglib/execute.c:2211)
  - [prepare_common](../p/prepare_common.md) (src/interfaces/ecpg/ecpglib/prepare.c:161-170)

## Notes and Other Information
- This structure serves as the central hub for all SQL statement operations in ECPG
- Supports both immediate execution and prepared statement scenarios
- The locale handling is conditionally compiled based on system capabilities
- Parameter arrays (paramvalues, paramlengths, paramformats) work together to provide comprehensive parameter binding
- Memory management is crucial as this structure contains multiple dynamically allocated components
- The structure bridges the gap between ECPG's C interface and PostgreSQL's libpq parameter binding system