# nameData

## Location
[src/include/c.h:740-742](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/c.h#L740-L742)

## Overview
The `nameData` struct is PostgreSQL's fundamental data type for storing identifiers such as table names, column names, function names, and other database object names. It provides a fixed-size, null-padded string representation that ensures consistent storage and handling of names throughout the system.

## Definition
```c
typedef struct nameData
{
    char        data[NAMEDATALEN];
} NameData;
typedef NameData *Name;
```

## Detailed Description
The `nameData` struct serves as PostgreSQL's primary mechanism for storing database identifiers. It is essentially a C string that is null-padded to exactly `NAMEDATALEN` bytes (64 bytes by default). This fixed-size approach ensures consistent memory layout and simplifies storage management across the entire PostgreSQL system.

The design is historically motivated but continues to serve important purposes:
- **Fixed memory footprint**: Every name occupies exactly `NAMEDATALEN` bytes, making memory calculations predictable
- **Null-termination safety**: The fixed size ensures proper null-termination handling
- **Storage consistency**: Names are stored consistently across catalogs, indexes, and memory structures

The actual usable length for identifiers is `NAMEDATALEN - 1` bytes because space must be reserved for the null terminator. By default, this means PostgreSQL identifiers are limited to 63 characters.

## Parameters / Member Variables
- `data[NAMEDATALEN]`: Fixed-size character array that stores the actual name string, null-padded to exactly `NAMEDATALEN` bytes

## Dependencies
- Constants used:
  - `NAMEDATALEN`: Defines the size of the data array (64 bytes by default, defined in pg_config_manual.h)
- Related types:
  - [NameData](../N/NameData.md): The typedef alias for the struct
  - `Name`: Pointer type to NameData (NameData *)
- Related macros:
  - `NameStr(name)`: Macro to access the data field as a C string

## Notes and Other Information
- **Historical context**: The use of a struct wrapper around a simple char array is historical but maintained for API consistency
- **Size limitation**: Changing `NAMEDATALEN` requires running `initdb` to reinitialize the database cluster
- **Memory efficiency**: The fixed-size design trades some memory efficiency for operational simplicity and consistency
- **Usage pattern**: Typically used through the `NameData` typedef rather than the struct name directly
- **String access**: The `NameStr()` macro provides convenient access to the underlying character data
- **Null-padding**: Names shorter than `NAMEDATALEN-1` are automatically null-padded to fill the entire structure