# typmap

## Location
[src/backend/bootstrap/bootstrap.c:143-162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L143-L162)

## Overview
A utility structure used during PostgreSQL's bootstrap process to cache type information from the pg_type catalog.

## Definition

```c
struct typmap
{								/* a hack */
	Oid			am_oid;
	FormData_pg_type am_typ;
};
```
## Detailed Description
The  structure is a bootstrap-time caching mechanism used to store type information during PostgreSQL's initialization phase. As noted by the comment "a hack", this is a specialized structure that serves as a temporary mapping between type OIDs and their complete pg_type catalog data.

This struct is used primarily in the bootstrap process to avoid repeated lookups to the pg_type system catalog when type information is needed. The structure stores both the OID of the type and a complete copy of the type's row data from pg_type, providing fast access to type metadata during database initialization.

## Parameters / Member Variables
- `am_oid`: The Object Identifier (OID) of the type from the pg_type catalog
- `am_typ`: A complete copy of the type's FormData_pg_type structure containing all type metadata from pg_type

## Dependencies
- Functions called/Symbols referenced:
  - MAXATTR (used in related arrays)
- Called from (representative examples):
  - [populate_typ_list](../p/populate_typ_list.md) (creates and populates typmap instances)
  - [gettype](../g/gettype.md) (searches through typmap instances)
  - [boot_get_type_io_data](../b/boot_get_type_io_data.md) (accesses typmap data)

## Notes and Other Information
- This structure is marked as "a hack" in the source code, indicating it's a pragmatic solution rather than an elegant design
- Used exclusively during bootstrap phase when the database system catalog is being constructed
- Stored in a global List called  for quick lookup during bootstrap operations
- The structure enables efficient type lookups without repeatedly querying the pg_type catalog during initialization
- Part of PostgreSQL's bootstrap machinery in 