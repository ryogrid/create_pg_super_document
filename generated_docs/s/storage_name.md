# storage_name

## Location
[src/backend/commands/tablecmds.c:2391-2469](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L2391-L2469)

## Overview
A static utility function that converts PostgreSQL storage type enumeration values to their corresponding human-readable string representations.

## Definition
static const char *storage_name(char c)

## Detailed Description
This function provides a mapping between PostgreSQL's internal storage type enumeration values and their string representations. PostgreSQL uses different storage strategies for different data types to optimize space usage and performance. The function takes a single character representing a storage type code and returns the corresponding descriptive string name.

The function handles four primary storage strategies used by PostgreSQL:
- PLAIN: Data is stored inline without compression or external storage
- EXTERNAL: Large data can be stored in external TOAST tables but without compression
- EXTENDED: Data can be compressed and/or stored externally in TOAST tables
- MAIN: Data is compressed but preferably kept inline rather than moved to external storage

This function is commonly used in debugging, error messages, and system catalog output where human-readable storage type names are needed.

## Parameters / Member Variables
- c: A character representing the storage type enumeration value (typstorage/attstorage)

## Dependencies
- Functions called/Symbols referenced:
  - TYPSTORAGE_PLAIN (enumeration constant for plain storage)
  - TYPSTORAGE_EXTERNAL (enumeration constant for external storage)  
  - TYPSTORAGE_EXTENDED (enumeration constant for extended storage)
  - TYPSTORAGE_MAIN (enumeration constant for main storage)
- Called from (representative examples):
  - [BuildDescForRelation](../B/BuildDescForRelation.md) (relation descriptor building)
  - [MergeChildAttribute](../M/MergeChildAttribute.md) (attribute inheritance processing)
  - [MergeInheritedAttribute](../M/MergeInheritedAttribute.md) (inherited attribute merging)

## Notes and Other Information
- This is a static function, only accessible within the tablecmds.c compilation unit
- Returns a string literal "???" for unrecognized storage type values, providing graceful handling of invalid input
- The function is used extensively in table definition and inheritance operations where storage types need to be displayed or compared
- Storage types are fundamental to PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for handling large data values

## Simplified Source
```c
static const char *storage_name(char c)
{
    switch (c) {
        case TYPSTORAGE_PLAIN:
            return "PLAIN";
        case TYPSTORAGE_EXTERNAL:
            return "EXTERNAL";
        case TYPSTORAGE_EXTENDED:
            return "EXTENDED";
        case TYPSTORAGE_MAIN:
            return "MAIN";
        default:
            return "???";
    }
}
```