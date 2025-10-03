# listOneExtensionContents

## Location
[src/bin/psql/describe.c:6120-6163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/describe.c#L6120-L6163)

## Overview
A static helper function that displays the detailed contents (objects) of a single PostgreSQL extension by querying the dependency catalog and using pg_describe_object to show human-readable object descriptions.

## Definition

```c
static bool
listOneExtensionContents(const char *extname, const char *oid)
```
## Detailed Description
This function generates and executes a SQL query to find all objects that belong to a specific extension by examining the pg_depend catalog. It uses the pg_describe_object function to convert the internal object identifiers into human-readable descriptions. The function creates a formatted title showing which extension's contents are being displayed and presents the results in a table format.

The query workflow:
1. Query pg_depend for objects with dependency type 'e' (extension) on the given extension OID
2. Use pg_describe_object to convert classid/objid pairs to readable descriptions
3. Order results alphabetically by description
4. Display with a custom title indicating the extension name

## Parameters / Member Variables
- `*extname`: The name of the extension (used for the display title)
- `*oid`: The OID of the extension (used in the WHERE clause to find dependent objects)
## Dependencies
- Functions called/Symbols referenced:
  - [PQExpBufferData](../P/PQExpBufferData.md) (data structure)
  - [printQueryOpt](../p/printQueryOpt.md) (data structure)
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md)
  - [PSQLexec](../P/PSQLexec.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [printQuery](../p/printQuery.md)
- Called from (representative examples):
  - [listExtensionContents](listExtensionContents.md) (in src/bin/psql/describe.c:6103)

## Notes and Other Information
- This is a static function, only accessible within the describe.c source file
- Uses pg_describe_object() system function to provide human-readable object descriptions
- The query filters by deptype = 'e' to find extension dependencies specifically
- Creates a custom title buffer for each extension to show "Objects in extension [name]"
- Uses internationalization support with gettext_noop and _() for proper localization
- Implements proper memory management by cleaning up PQExpBuffer objects
- Returns false only on query execution failure, otherwise always returns true
- The function is designed to be called iteratively for multiple extensions