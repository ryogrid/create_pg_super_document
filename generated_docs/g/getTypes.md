# getTypes

## Location
[src/bin/pg_dump/pg_dump.c:5847-6017](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L5847-L6017)

## Overview
Reads all data types from the PostgreSQL system catalogs and returns them as an array of TypeInfo structures for pg_dump processing, including built-in, user-defined, and array types.

## Definition

```c
TypeInfo *
getTypes(Archive *fout, int *numTypes)
```
## Detailed Description
This function is a comprehensive data type collection component of pg_dump that queries the pg_type system catalog to retrieve information about all data types in the database. It handles various type categories including built-in types, user-defined types, domains, composite types, enums, ranges, multiranges, and arrays.

Key operations performed:
1. Executes a complex SQL query joining pg_type with pg_class to get complete type metadata
2. Detects array types using element type relationships and naming patterns
3. Creates TypeInfo structures with proper dump object initialization and namespace resolution
4. Handles ACL information and determines dumpability based on dump options
5. Special processing for domain types to fetch constraint information
6. Creates shell type objects for base and range types that need I/O function definitions
7. Manages type dependencies and dump ordering requirements

The function must run after getFuncs() because it relies on function lookup capabilities for type dependencies.

## Parameters / Member Variables
- : Archive structure containing dump configuration and output methods
- : Output parameter that receives the total number of types found

## Dependencies
- Functions called/Symbols referenced:
  - [ExecuteSqlQuery](../E/ExecuteSqlQuery.md)
  - atooid
  - [AssignDumpId](../A/AssignDumpId.md)
  - [findNamespace](../f/findNamespace.md)
  - [getRoleName](getRoleName.md)
  - [selectDumpableType](../s/selectDumpableType.md)
  - [getDomainConstraints](getDomainConstraints.md)
  - [pg_malloc](../p/pg_malloc.md)
  - [pg_strdup](../p/pg_strdup.md)
- Called from (representative examples):
  - [getSchemaData](getSchemaData.md)

## Notes and Other Information
- Must run after getFuncs() due to function dependency requirements
- Includes built-in types as they may be used as array elements by user-defined types
- Detects auto-generated array types reliably using element type's typarray field
- Creates shell type objects for base and range types needing I/O functions
- Handles domain constraints via getDomainConstraints for domain types
- Sets DUMP_COMPONENT_ACL flag for types with ACL information
- Uses complex SQL to determine array types and relation kinds
- Memory allocation uses pg_malloc for both TypeInfo and ShellTypeInfo arrays
- Returns allocated array that must be freed by caller
- Essential for complete type system representation during database dumps