# ecpg_do_prologue

## Location
[src/interfaces/ecpg/ecpglib/execute.c:1944-2210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L1944-L2210)

## Overview
Initializes the execution infrastructure for ECPG statements by creating statement structures, setting numeric locale, and preprocessing variable lists.

## Definition

```c
bool
ecpg_do_prologue(int lineno, const int compat, const int force_indicator,
				 const char *connection_name, const bool questionmarks,
				 enum ECPG_statement_type statement_type, const char *query,
				 va_list args, struct statement **stmt_out)
```
## Detailed Description
This function performs critical initialization tasks before executing any ECPG statement. It serves as the setup phase that prepares all necessary infrastructure:

**Key responsibilities:**
- Creates and initializes statement structure with execution context
- Establishes database connection and validates connectivity  
- Sets the C numeric locale to ensure proper decimal point handling for database communication
- Processes variable argument lists into structured input/output variable chains
- Handles prepared statement setup for ECPGst_prepnormal and ECPGst_execute types
- Performs extensive validation of parameters and connection state
- Manages memory allocation with proper cleanup on errors

The function processes complex variable argument lists containing type information, pointers, sizes, and indicator variables, organizing them into linked lists for later processing by execution and result handling functions.

## Parameters / Member Variables
- `lineno`: Source line number for error reporting and debugging
- `compat`: Compatibility mode (e.g., Informix compatibility settings)
- `force_indicator`: Flag controlling indicator variable behavior
- `*connection_name`: Database connection identifier (NULL for default connection)
- `questionmarks`: Boolean indicating whether query uses ? parameter placeholders
- `statement_type`: Type of SQL statement (prepare, execute, normal, etc.)
- `*query`: SQL command string to execute or prepare
- `args`: Variable argument list containing input/output variable specifications
- `**stmt_out`: Output parameter returning initialized statement structure
## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_pthreads_init](ecpg_pthreads_init.md): Initializes threading support
  - [ecpg_get_connection](ecpg_get_connection.md): Retrieves database connection
  - [ecpg_init](ecpg_init.md): Initializes connection state
  - [ecpg_alloc](ecpg_alloc.md): Allocates memory with error handling
  - uselocale/setlocale: Manages numeric locale for database communication
  - [ecpg_auto_prepare](ecpg_auto_prepare.md): Handles automatic statement preparation
  - [ecpg_prepared](ecpg_prepared.md): Retrieves prepared statement text
  - [ecpg_strdup](ecpg_strdup.md): Duplicates strings with error handling
  - [ecpg_clear_auto_mem](ecpg_clear_auto_mem.md): Initializes automatic memory management
  - [ecpg_do_epilogue](ecpg_do_epilogue.md): Cleanup function called on errors
  - [ecpg_raise](ecpg_raise.md): Error reporting function
- Called from (representative examples):
  - [ecpg_do](ecpg_do.md): Main ECPG statement execution entry point

## Notes and Other Information
- Returns true on successful initialization, false on any failure
- Automatically calls ecpg_do_epilogue() for cleanup when errors occur
- Thread-safe locale handling using uselocale() when available, falls back to setlocale()
- Supports complex variable specifications including arrays, indicators, and various data types
- Critical foundation function that must succeed before any statement execution
- Handles both simple and complex prepared statement scenarios
- Essential component of ECPG's statement execution pipeline

## Simplified Source

```c
bool
ecpg_do_prologue(int lineno, const int compat, const int force_indicator,
                 const char *connection_name, const bool questionmarks,
                 enum ECPG_statement_type statement_type, const char *query,
                 va_list args, struct statement **stmt_out)
{
    struct statement *stmt = NULL;
    struct connection *con;
    enum ECPGttype type;
    struct variable **list;
    char *prepname;
    bool is_prepared_name_set;

    *stmt_out = NULL;

    // Validate query
    if (!query)
    {
        ecpg_raise(lineno, ECPG_EMPTY, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR, NULL);
        return false;
    }

    // Initialize threading and get connection
    ecpg_pthreads_init();
    con = ecpg_get_connection(connection_name);

    if (!ecpg_init(con, connection_name, lineno))
        return false;

    // Allocate statement structure
    stmt = (struct statement *) ecpg_alloc(sizeof(struct statement), lineno);
    if (!stmt) return false;

    // Set C locale for numeric communication with database
#ifdef HAVE_USELOCALE
    stmt->oldlocale = uselocale(ecpg_clocale);
    if (stmt->oldlocale == (locale_t) 0)
    {
        ecpg_do_epilogue(stmt);
        return false;
    }
#else
    stmt->oldlocale = ecpg_strdup(setlocale(LC_NUMERIC, NULL), lineno);
    if (!stmt->oldlocale)
    {
        ecpg_do_epilogue(stmt);
        return false;
    }
    setlocale(LC_NUMERIC, "C");
#endif

    // Handle prepared statements
    if (statement_type == ECPGst_prepnormal)
    {
        if (!ecpg_auto_prepare(lineno, connection_name, compat, &prepname, query))
        {
            ecpg_do_epilogue(stmt);
            return false;
        }
        stmt->command = prepname;
        statement_type = ECPGst_execute;
    }
    else
        stmt->command = ecpg_strdup(query, lineno);

    stmt->name = NULL;

    // Handle EXECUTE statements
    if (statement_type == ECPGst_execute)
    {
        char *command = ecpg_prepared(stmt->command, con);
        if (command)
        {
            stmt->name = stmt->command;
            stmt->command = ecpg_strdup(command, lineno);
        }
        else
        {
            ecpg_raise(lineno, ECPG_INVALID_STMT,
                      ECPG_SQLSTATE_INVALID_SQL_STATEMENT_NAME, stmt->command);
            ecpg_do_epilogue(stmt);
            return false;
        }
    }

    // Initialize statement fields
    stmt->connection = con;
    stmt->lineno = lineno;
    stmt->compat = compat;
    stmt->force_indicator = force_indicator;
    stmt->questionmarks = questionmarks;
    stmt->statement_type = statement_type;

    // Process variable arguments to create input/output lists
    is_prepared_name_set = false;
    list = &(stmt->inlist);
    type = va_arg(args, enum ECPGttype);

    while (type != ECPGt_EORT)
    {
        if (type == ECPGt_EOIT)
            list = &(stmt->outlist);  // Switch to output variables
        else
        {
            // Create variable structure
            struct variable *var = (struct variable *) ecpg_alloc(sizeof(struct variable), lineno);
            if (!var)
            {
                ecpg_do_epilogue(stmt);
                return false;
            }

            // Extract variable information from arguments
            var->type = type;
            var->pointer = va_arg(args, char *);
            var->varcharsize = va_arg(args, long);
            var->arrsize = va_arg(args, long);
            var->offset = va_arg(args, long);

            // Set value pointer based on array/pointer characteristics
            if (var->arrsize == 0 ||
                (var->varcharsize == 0 && ((var->type != ECPGt_char && var->type != ECPGt_unsigned_char) || (var->arrsize <= 1))))
                var->value = *((char **) (var->pointer));
            else
                var->value = var->pointer;

            // Handle negative sizes (unbounded arrays)
            if (var->arrsize < 0) var->arrsize = 0;
            if (var->varcharsize < 0) var->varcharsize = 0;

            var->next = NULL;

            // Extract indicator variable information
            var->ind_type = va_arg(args, enum ECPGttype);
            var->ind_pointer = va_arg(args, char *);
            var->ind_varcharsize = va_arg(args, long);
            var->ind_arrsize = va_arg(args, long);
            var->ind_offset = va_arg(args, long);

            // Set indicator value pointer
            if (var->ind_type != ECPGt_NO_INDICATOR &&
                (var->ind_arrsize == 0 || var->ind_varcharsize == 0))
                var->ind_value = *((char **) (var->ind_pointer));
            else
                var->ind_value = var->ind_pointer;

            // Handle negative indicator sizes
            if (var->ind_arrsize < 0) var->ind_arrsize = 0;
            if (var->ind_varcharsize < 0) var->ind_varcharsize = 0;

            // Validate variable
            if (!var->pointer)
            {
                ecpg_raise(lineno, ECPG_INVALID_STMT,
                          ECPG_SQLSTATE_INVALID_SQL_STATEMENT_NAME, NULL);
                ecpg_free(var);
                ecpg_do_epilogue(stmt);
                return false;
            }

            // Add to appropriate list
            struct variable *ptr;
            for (ptr = *list; ptr && ptr->next; ptr = ptr->next);
            if (!ptr)
                *list = var;
            else
                ptr->next = var;

            // Set prepared statement name if needed
            if (!is_prepared_name_set && stmt->statement_type == ECPGst_prepare)
            {
                stmt->name = ecpg_strdup(var->value, lineno);
                is_prepared_name_set = true;
            }
        }

        type = va_arg(args, enum ECPGttype);
    }

    // Validate connection
    if (!con || !con->connection)
    {
        ecpg_raise(lineno, ECPG_NOT_CONN, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR,
                  (con) ? con->name : ecpg_gettext("<empty>"));
        ecpg_do_epilogue(stmt);
        return false;
    }

    // Validate prepared statement name
    if (!is_prepared_name_set && stmt->statement_type == ECPGst_prepare)
    {
        ecpg_raise(lineno, ECPG_TOO_FEW_ARGUMENTS, ECPG_SQLSTATE_ECPG_INTERNAL_ERROR,
                  (con) ? con->name : ecpg_gettext("<empty>"));
        ecpg_do_epilogue(stmt);
        return false;
    }

    // Initialize automatic memory management
    ecpg_clear_auto_mem();

    *stmt_out = stmt;
    return true;
}
```