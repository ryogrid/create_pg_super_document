# pqReleaseConnHosts

## Location
[src/interfaces/libpq/fe-connect.c:4716-4744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L4716-L4744)

## Overview
Frees the host list data structure in a PGconn, including all associated host connection information and securely clearing any stored passwords.

## Definition


## Detailed Description
The  function is responsible for deallocating the connection host list () array within a PGconn structure. This function handles multiple host configurations where PostgreSQL clients can specify multiple potential database servers for connection failover or load balancing.

For each host entry in the connection host array, the function:
1. Frees the hostname string
2. Frees the host address string  
3. Frees the port string
4. Securely clears and frees password information using 
5. Finally frees the entire  array itself

The function pays special attention to security by using  to overwrite password data in memory before freeing it, preventing sensitive authentication information from persisting in freed memory blocks.

## Parameters / Member Variables
- : A pointer to the PGconn structure containing the host list to be freed

## Dependencies
- Functions called/Symbols referenced:
  -  (secure memory clearing)
  -                total        used        free      shared  buff/cache   available
Mem:        32819372     7238504    24332168       13740     1248700    25194156
Swap:        8388608           0     8388608 (memory deallocation)
  -  (string length calculation)

- Called from (representative examples):
  - 
  - 

## Notes and Other Information
- Safely handles NULL  pointer by checking before processing
- Iterates through all entries in the host array ( entries)
- Each host entry contains , , , and optionally  fields
- Uses  specifically for password fields to prevent sensitive data from remaining in memory
- The function is part of the connection cleanup process and is typically called when a connection is being torn down
- Supports PostgreSQL's multi-host connection feature where multiple servers can be specified for failover
- Does not reset  or  pointer after freeing - caller responsibility
- Password field is checked for NULL before attempting to clear and free it
- All string fields are freed using standard  except passwords which get secure clearing first