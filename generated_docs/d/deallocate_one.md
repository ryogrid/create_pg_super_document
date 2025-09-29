# deallocate_one

## Location
[src/interfaces/ecpg/ecpglib/prepare.c:260-314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/prepare.c#L260-L314)

## Overview
A static function that deallocates a single prepared statement from the ECPG (Embedded SQL in C for PostgreSQL) library, removing both the backend prepared statement and freeing associated memory resources.

## Definition

```c
static bool
deallocate_one(int lineno, enum COMPAT_MODE c, struct connection *con,
			   struct prepared_statement *prev, struct prepared_statement *this)
```
## Detailed Description
The  function performs the complete deallocation of a prepared statement in the ECPG library. It operates in two phases: first sending a DEALLOCATE command to the PostgreSQL backend to remove the prepared statement from the server, and then freeing all client-side memory resources associated with the statement. The function handles different compatibility modes and manages the linked list structure of prepared statements by properly updating pointers when removing a statement from the chain.

## Parameters / Member Variables
- : Line number in the source code where the deallocation is requested (for error reporting)
- : Compatibility mode enumeration that affects error handling behavior
- : Pointer to the database connection structure
- : Pointer to the previous prepared statement in the linked list (NULL if this is the first statement)
- : Pointer to the prepared statement to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [ecpg_log](../e/ecpg_log.md) (logging functionality)
  - [ecpg_alloc](../e/ecpg_alloc.md) (memory allocation)
  - [PQexec](../P/PQexec.md) (PostgreSQL command execution)
  - [ecpg_free](../e/ecpg_free.md) (memory deallocation)
  - [ecpg_check_PQresult](../e/ecpg_check_PQresult.md) (result validation)
  - INFORMIX_MODE (compatibility mode check)
  - [ecpg_raise](../e/ecpg_raise.md) (error reporting)
- Called from (representative examples):
  - [ECPGdeallocate](../E/ECPGdeallocate.md)
  - [ecpg_deallocate_all_conn](../e/ecpg_deallocate_all_conn.md)
  - [ecpg_freeStmtCacheEntry](../e/ecpg_freeStmtCacheEntry.md)

## Notes and Other Information
- The function ignores backend deallocation errors in INFORMIX compatibility mode to maintain compatibility with legacy behavior
- Memory cleanup is performed regardless of backend deallocation success to prevent memory leaks
- The function properly maintains the linked list integrity by updating the previous statement's next pointer or the connection's prep_stmts head pointer
- Error handling includes raising ECPG_INVALID_STMT errors for invalid statement names in non-INFORMIX modes

## Simplified Source

```c
static bool deallocate_one(int lineno, enum COMPAT_MODE c, struct connection *con,
                          struct prepared_statement *prev, struct prepared_statement *this) {
    bool success = false;

    // Log the deallocation attempt
    ecpg_log("deallocate_one on line %d: name %s\n", lineno, this->name);

    // Deallocate the statement in the backend database
    if (this->prepared) {
        // Build DEALLOCATE command
        char *text = (char *) ecpg_alloc(strlen("deallocate \"\" ") + strlen(this->name),
                                        this->stmt->lineno);
        if (text) {
            sprintf(text, "deallocate \"%s\"", this->name);

            // Execute deallocate command
            PGresult *query = PQexec(this->stmt->connection->connection, text);
            ecpg_free(text);

            // Check if command succeeded
            if (ecpg_check_PQresult(query, lineno, this->stmt->connection->connection,
                                   this->stmt->compat)) {
                PQclear(query);
                success = true;
            }
        }
    }

    // Handle error cases (except in INFORMIX mode)
    if (!success && !INFORMIX_MODE(c)) {
        ecpg_raise(lineno, ECPG_INVALID_STMT, ECPG_SQLSTATE_INVALID_SQL_STATEMENT_NAME,
                   this->name);
        return false;
    }

    // Free all client-side resources
    ecpg_free(this->stmt->command);
    ecpg_free(this->stmt);
    ecpg_free(this->name);

    // Remove from linked list
    if (prev != NULL)
        prev->next = this->next;
    else
        con->prep_stmts = this->next;

    ecpg_free(this);
    return true;
}
```