# _PQconninfoOption

## Location
src/interfaces/libpq/libpq-fe.h: 255 - 267

## Overview
The  struct represents a single connection parameter definition used by PostgreSQL's libpq library. It contains metadata about connection options including their keywords, default values, and display characteristics.

## Definition


## Detailed Description
The  structure defines the metadata for a single PostgreSQL connection parameter. It is used by functions like  and  to provide information about available connection options. Each instance describes one connection parameter including its keyword name, fallback mechanisms (environment variables and compiled defaults), current value, and display characteristics for GUI applications. The structure supports a hierarchical fallback system where values can come from explicit settings, environment variables, or compiled-in defaults.

## Parameters / Member Variables
- : The parameter name used in connection strings (e.g., "host", "port", "dbname")
- : Name of environment variable to check for default value (e.g., "PGHOST", "PGPORT")
- : Built-in default value compiled into libpq
- : Current effective value of the parameter, NULL if not set
- : Human-readable label for use in connection dialogs
- : Display type indicator: "" for normal text, "*" for password fields, "D" for debug options
- : Suggested field width in characters for GUI display

## Dependencies
- Functions called/Symbols referenced:
  - (Uses standard C types only)
- Called from (representative examples):
  - PQconndefaults() (returns array of these structures)
  - [PQconninfoParse](PQconninfoParse.md)() (parses connection strings into these structures)

## Notes and Other Information
- All fields except  point to static strings that must not be modified
- The  field may be NULL or point to a malloc'd string that will be freed by PQconninfoFree()
- Arrays of these structures are terminated by an entry with NULL keyword
- The structure is part of libpq's public API for introspecting connection parameters
- Display characteristics support building user-friendly connection configuration interfaces
- The fallback hierarchy allows flexible configuration through multiple sources