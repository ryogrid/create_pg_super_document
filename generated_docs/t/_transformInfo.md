# _transformInfo

## Location
[src/bin/pg_dump/pg_dump.h:517-523](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.h#L517-L523)

## Overview
The  structure represents transform information in PostgreSQL's pg_dump utility, storing metadata about how data types are converted between SQL and procedural languages.

## Definition

```c
typedef struct _transformInfo
{
	DumpableObject dobj;
	Oid			trftype;
	Oid			trflang;
	Oid			trffromsql;
	Oid			trftosql;
} TransformInfo;
```
## Detailed Description
The  structure captures the definition of transforms, which are special functions that define how a data type should be converted when passing between SQL and a procedural language. Transforms enable custom data types to work seamlessly with procedural languages by providing explicit conversion routines in both directions (SQL to language and language to SQL).

## Parameters / Member Variables
- : Base  containing common metadata for dump operations
- : OID of the data type that this transform applies to
- : OID of the procedural language that this transform applies to
- : OID of the function used to convert from SQL representation to the procedural language representation
- : OID of the function used to convert from the procedural language representation back to SQL representation

## Dependencies
- Functions called/Symbols referenced:
  - DumpableObject
- Called from (representative examples):
  - No direct references found in current analysis

## Notes and Other Information
- Transforms are particularly useful for complex data types (like JSON, XML, or custom composite types) that need special handling in procedural languages
- The  function converts values from the SQL type's internal representation to a form suitable for the procedural language
- The  function performs the reverse conversion, taking values from the procedural language and converting them to the SQL type's internal representation
- Both transform functions are optional; if not specified, the procedural language will use its default conversion mechanism
- This structure enables pg_dump to preserve custom transform definitions during database backup and restoration operations
- Transforms provide a way to optimize data type handling in procedural languages while maintaining type safety