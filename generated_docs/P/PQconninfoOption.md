# PQconninfoOption

## Location
[src/interfaces/libpq/libpq-fe.h:268-282](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/libpq-fe.h#L268-L282)

## Overview
PQconninfoOption is a structure that represents a single connection parameter option in PostgreSQL's libpq client library, containing metadata about connection parameters including their values, labels, and display characteristics.

## Definition


## Detailed Description
PQconninfoOption is a fundamental data structure in PostgreSQL's libpq interface that encapsulates all metadata necessary for handling connection parameters. This structure is used extensively throughout the connection establishment process, connection string parsing, and user interface components that need to display or manipulate connection options. Each instance represents a single connection parameter (like host, port, database name, etc.) with complete information about how it should be handled, displayed, and what its current value is.

The structure serves multiple purposes: it provides a standardized way to represent connection options, supports internationalization through labels, enables secure handling of sensitive data through display characteristics, and maintains fallback mechanisms through environment variables and compiled defaults.

## Parameters / Member Variables
- : The canonical name of the connection parameter (e.g., "host", "port", "dbname")
- : Name of the environment variable that can provide a fallback value for this parameter (e.g., "PGHOST" for host)
- : The default value compiled into libpq for this parameter, used when no other value is provided
- : The current value of the parameter, or NULL if no value has been set
- : Human-readable label for this parameter, used in connection dialogs and user interfaces
- : Display characteristic control string - "" for normal display, "*" for password fields (hidden), "D" for debug options (hidden by default)
- : Suggested field width in characters for displaying this parameter in dialog boxes

## Dependencies
- Functions called/Symbols referenced:
  - (This structure doesn't directly call functions but is used by connection-related functions)
- Called from (representative examples):
  - PQconndefaults
  - [PQconninfoParse](PQconninfoParse.md)
  - [PQconninfo](PQconninfo.md)
  - [PQconninfoFree](PQconninfoFree.md)
  - [conninfo_init](../c/conninfo_init.md)
  - [conninfo_parse](../c/conninfo_parse.md)
  - [parseServiceInfo](../p/parseServiceInfo.md)

## Notes and Other Information
- This structure is central to libpq's connection parameter management system
- The dispchar field enables secure handling of passwords and selective display of debug options
- Used extensively in PostgreSQL client tools like psql, pg_dump, and pg_basebackup
- The structure supports both programmatic access and user interface generation
- Memory management for the string fields is handled by libpq's connection info functions
- Arrays of these structures are typically terminated by an entry with a NULL keyword field