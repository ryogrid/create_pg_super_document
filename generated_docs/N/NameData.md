# NameData

## Location
[src/include/c.h:743-743](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/c.h#L743-L743)

## Overview
A fixed-size structure representing PostgreSQL identifier names (such as table names, column names, function names) with a maximum length of NAMEDATALEN bytes, designed for efficient storage and comparison in the system catalogs.

## Definition
```c
typedef struct nameData
{
    char        data[NAMEDATALEN];
} NameData;
typedef NameData *Name;

#define NameStr(name)   ((name).data)
```

## Detailed Description
NameData is PostgreSQL's fundamental structure for storing database object identifiers throughout the system catalogs. It represents a fixed-size character array of exactly NAMEDATALEN (64) bytes, null-padded to ensure consistent storage size. This structure is used extensively in PostgreSQL's catalog tables to store names of databases, schemas, tables, columns, functions, operators, types, and other database objects. The fixed-size nature enables efficient indexing and comparison operations in the catalog system, while the null-padding ensures that shorter names occupy the same storage space as longer ones for consistent row sizing.

## Parameters / Member Variables
- `data[NAMEDATALEN]`: Fixed-size character array of 64 bytes containing the identifier name, null-terminated and null-padded

## Dependencies
- Functions called/Symbols referenced:
  - NAMEDATALEN (defined as 64 in pg_config_manual.h)
- Called from (representative examples):
  - All PostgreSQL catalog tables (pg_class, pg_attribute, pg_proc, pg_namespace, etc.)
  - [namerecv](../n/namerecv.md) (input function for Name type)
  - [NameGetDatum](NameGetDatum.md) (conversion to PostgreSQL Datum type)
  - [CatCacheCopyKeys](../C/CatCacheCopyKeys.md) (catalog cache key copying)
  - [ReplicationSlotPersistentData](../R/ReplicationSlotPersistentData.md) (replication slot names)
  - Various catalog creation and manipulation functions

## Notes and Other Information
- The actual usable length for names is NAMEDATALEN-1 (63 characters) because one byte is reserved for the null terminator
- Changing NAMEDATALEN requires a complete database reinitialization (initdb)
- The structure uses a historical design where a simple char array is wrapped in a struct for type safety
- The NameStr() macro provides convenient access to the underlying character data
- Used as the standard type for all PostgreSQL identifier names throughout the system
- Names longer than the limit are automatically truncated during input processing
- The fixed-size design enables efficient B-tree indexing and sorting operations in catalog tables
- Comparison operations can use standard string comparison functions with the fixed-size guarantee