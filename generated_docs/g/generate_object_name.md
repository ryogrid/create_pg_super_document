# generate_object_name

## Location
src/bin/pg_basebackup/pg_createsubscriber.c: 685 - 733

## Overview
Generates unique object names for PostgreSQL logical replication objects (publications, subscriptions, replication slots) when not explicitly specified by the user.

## Definition


## Detailed Description
The  function creates unique names for PostgreSQL logical replication objects by combining a fixed prefix with the database OID and a random hexadecimal number. This naming scheme ensures uniqueness across different databases and multiple invocations of pg_createsubscriber. The function queries the current database's OID from the system catalog and combines it with a pseudo-random number generated using PostgreSQL's internal PRNG to create names like "pg_createsubscriber_16384_a1b2c3d4".

The generated names are designed to fit within PostgreSQL's  limit (typically 64 characters) and follow a predictable pattern that makes them easily identifiable as pg_createsubscriber-generated objects. The function includes comprehensive error handling for database queries and ensures exactly one result row is returned.

## Parameters / Member Variables
- : An active PostgreSQL database connection used to query the current database's OID

## Dependencies
- Functions called/Symbols referenced:
  - PQexec (libpq function for executing SQL queries)
  - PGRES_TUPLES_OK (libpq constant for successful SELECT result)
  - PQresultErrorMessage (libpq function to get error message from result)
  - PQntuples (libpq function to get number of rows in result)
  - PQgetvalue (libpq function to get field value from result)
  - PQclear (libpq function to free result memory)
  - strtoul (standard C function to convert string to unsigned long)
  - pg_prng_uint32 (PostgreSQL pseudo-random number generator function)
  - psprintf (PostgreSQL string formatting function)
  - disconnect_database (utility function for connection cleanup on error)

- Called from (representative examples):
  - setup_publisher (function that creates publication objects)
  - LogicalRepInfo structure operations

## Notes and Other Information
- This is a static function, only accessible within pg_createsubscriber.c
- The naming pattern is: "pg_createsubscriber_{database_oid}_{random_hex}"
- Maximum name length is constrained to fit within NAMEDATALEN - 1 (typically 63 characters)
- Current schema uses maximum 40 characters: "pg_createsubscriber" (20) + "_" (1) + OID (up to 10 digits) + "_" (1) + random hex (8 characters) + null terminator
- Uses PostgreSQL's internal PRNG state () for random number generation
- Query targets  to get the current database's OID
- Error conditions result in program termination via 
- Located in src/bin/pg_basebackup/pg_createsubscriber.c:685-733
- The returned string is allocated with psprintf and should be freed by the caller
- Useful for creating publications, subscriptions, and replication slots when user doesn't provide explicit names
- Database OID provides database-specific uniqueness, while random number provides temporal uniqueness