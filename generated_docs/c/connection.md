# connection

## Location
[src/interfaces/ecpg/ecpglib/ecpglib_extern.h:104-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/ecpglib_extern.h#L104-L114)

## Overview
The `connection` struct represents a database connection in the ECPG (Embedded SQL in C for PostgreSQL) library, storing connection metadata and maintaining connection state information.

## Definition
```c
struct connection
{
    char       *name;
    PGconn     *connection;
    bool        autocommit;
    struct ECPGtype_information_cache *cache_head;
    struct prepared_statement *prep_stmts;
    struct connection *next;
};
```

## Detailed Description
This structure is the core data type for managing database connections in ECPG applications. It encapsulates all necessary information for a single database connection, including the actual PostgreSQL connection handle, connection metadata, caching mechanisms, and prepared statement management. The structure forms a linked list to support multiple simultaneous connections within a single application.

## Parameters / Member Variables
- `name`: String identifier for the connection, allowing applications to reference connections by name
- `connection`: Pointer to the actual PGconn structure from libpq that handles the database connection
- `autocommit`: Boolean flag indicating whether transactions should be automatically committed
- `cache_head`: Pointer to the head of a linked list containing cached type information for this connection
- `prep_stmts`: Pointer to the head of a linked list containing prepared statements associated with this connection
- `next`: Pointer to the next connection structure, enabling multiple connections to be managed in a linked list

## Dependencies
- Functions called/Symbols referenced:
  - [ECPGtype_information_cache](../E/ECPGtype_information_cache.md)
  - [prepared_statement](../p/prepared_statement.md)
- Called from (representative examples):
  - No direct references found in the current analysis

## Notes and Other Information
This structure is defined in the ECPG library external header file, indicating it is part of the public interface for ECPG applications. The linked list design allows applications to maintain multiple named database connections simultaneously, which is essential for complex applications that need to interact with multiple databases or use different connection parameters for different operations.