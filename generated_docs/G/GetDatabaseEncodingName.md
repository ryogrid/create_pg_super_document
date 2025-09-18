# GetDatabaseEncodingName

## Location
src/backend/utils/mb/mbutils.c: 1267 - 1272

## Overview
Returns the string name of the current database encoding, providing a human-readable representation of the database's character encoding.

## Definition
const char *GetDatabaseEncodingName(void)

## Detailed Description
This function provides access to the string name of the database encoding (also called server encoding). While GetDatabaseEncoding() returns a numeric identifier, this function returns the corresponding canonical name as a string (e.g., "UTF8", "LATIN1", "SQL_ASCII").

The function is a simple accessor that returns the name field from the global DatabaseEncoding structure. The returned string is a constant that represents the official PostgreSQL name for the encoding, which can be used for display purposes, logging, error messages, and interfacing with external systems that expect encoding names rather than numeric identifiers.

This function is commonly used when PostgreSQL needs to communicate encoding information to external libraries, log encoding information, or validate encoding compatibility.

## Parameters / Member Variables
None - this is a parameter-less function.

## Dependencies
- Functions called/Symbols referenced:
  - DatabaseEncoding (global structure containing encoding information)
- Called from (representative examples):
  - get_collation_oid (src/backend/catalog/namespace.c:4017)
  - check_client_encoding (src/backend/commands/variable.c:715)
  - libpqrcv_connect (src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:180)
  - CheckMyDatabase (src/backend/utils/init/postinit.c:399)
  - InitializeClientEncoding (src/backend/utils/mb/mbutils.c:299)
  - locate_stem_module (src/backend/snowball/dict_snowball.c:216)

## Notes and Other Information
- Returns a const char* pointing to a static string - should not be freed or modified
- The returned name is the canonical PostgreSQL encoding name, not necessarily the same as system locale names
- Commonly used in error messages and logging to provide human-readable encoding information
- Essential for interfacing with external libraries that require encoding names as strings
- Used in replication connections to ensure encoding compatibility between primary and standby servers
- The encoding name remains constant throughout the database session