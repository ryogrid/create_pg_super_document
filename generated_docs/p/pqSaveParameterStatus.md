# pqSaveParameterStatus

## Location
src/interfaces/libpq/fe-exec.c: 1081 - 1205

## Overview
pqSaveParameterStatus stores and manages server parameter status information received from the PostgreSQL backend, maintaining a linked list of parameter name-value pairs and updating connection-specific cached values for critical parameters.

## Definition


## Detailed Description
This function manages the storage of server parameter status information in a PGconn structure. It first removes any existing entry for the same parameter name, then creates a new entry with the updated value. The function uses a space-efficient single malloc allocation that stores the pgParameterStatus structure, parameter name, and value in one contiguous block.

Additionally, the function maintains cached copies of several critical parameters directly in the PGconn structure for performance reasons. These include client_encoding, standard_conforming_strings, server_version (converted to numeric form), default_transaction_read_only, in_hot_standby, and scram_iterations. Some parameters like client_encoding and standard_conforming_strings are also stored in static variables to support legacy functions like PQescapeString and PQescapeBytea in single-connection programs.

## Parameters / Member Variables
- : Pointer to the PGconn structure that will store the parameter status
- : String containing the parameter name (e.g., "client_encoding", "server_version")
- : String containing the parameter value as sent by the server

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for allocating parameter status structures)
  - free (for removing old parameter entries)
  - strcmp (for parameter name comparisons)
  - strcpy (for copying parameter strings)
  - strlen (for calculating string lengths)
  - pg_char_to_encoding (for converting encoding names)
  - sscanf (for parsing server version)
  - atoi (for parsing numeric values)
- Types used:
  - pgParameterStatus (linked list node structure)
- Constants used:
  - PG_SQL_ASCII (fallback encoding)
  - PG_BOOL_YES, PG_BOOL_NO (boolean value constants)
- Called from:
  - getParameterStatus (in fe-protocol3.c for processing server messages)

## Notes and Other Information
- Uses a linked list to store parameter status information, with new entries prepended to the list
- Implements space-efficient allocation: stores structure, name, and value in a single malloc block
- Automatically removes old entries for the same parameter before adding new ones
- Maintains cached copies of critical parameters in PGconn fields for quick access
- Handles server version parsing for both old format (9.6.1) and new format (10.1) version numbers
- Updates static variables for client_encoding and standard_conforming_strings to support legacy escape functions
- Server version is converted to numeric format: major*10000 + minor*100 + revision for old style, or major*10000 + minor for new style
- Boolean parameters are converted from string "on"/"off" to PG_BOOL_YES/PG_BOOL_NO constants
- Memory allocation failure is handled gracefully - the function continues even if malloc fails
- Parameter names and values are copied, not referenced, ensuring the connection owns the data
- The function supports various PostgreSQL configuration parameters including encoding settings, version info, transaction settings, and authentication parameters