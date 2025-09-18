# conninfo_getval

## Location
[src/interfaces/libpq/fe-connect.c:6838-6863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-connect.c#L6838-L6863)

## Overview
Retrieves the value of a connection option by keyword from a PQconninfoOption array.

## Definition


## Detailed Description
This utility function provides a simple interface for looking up connection option values by their keyword names. It acts as a wrapper around the conninfo_find function, extracting the value field from the found option structure.

The function performs the following operations:
1. Calls conninfo_find to locate the option with the specified keyword
2. Returns the option's value if found
3. Returns NULL if no matching option exists

This is a read-only operation that does not modify the connection options array and provides a clean interface for accessing stored connection parameters.

## Parameters / Member Variables
- : Array of PQconninfoOption structures to search
- : The keyword name of the connection option to retrieve

## Dependencies
- Functions called/Symbols referenced:
  - [conninfo_find](conninfo_find.md)
  - [PQconninfoOption](../P/PQconninfoOption.md) (struct type)
- Called from (representative examples):
  - [fillPGconn](../f/fillPGconn.md)
  - [parseServiceInfo](../p/parseServiceInfo.md)
  - internalPQconninfoOption

## Notes and Other Information
- Returns pointer to option value string on success, NULL if option not found
- Does not create copies of the returned string - returns pointer to internal storage
- Caller should not modify or free the returned string
- The returned pointer remains valid as long as the connOptions array is valid
- Used throughout libpq for retrieving connection parameters during connection establishment
- Thread-safe as long as the underlying connOptions array is not being modified concurrently