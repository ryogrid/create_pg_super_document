# LogicalRepTyp

## Location
[src/include/replication/logicalproto.h:119-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/logicalproto.h#L119-L124)

## Overview
LogicalRepTyp is a structure that stores type mapping information for custom data types used in logical replication, enabling proper type handling between publisher and subscriber.

## Definition

```c
typedef struct LogicalRepTyp
{
	Oid			remoteid;		/* unique id of the remote type */
	char	   *nspname;		/* schema name of remote type */
	char	   *typname;		/* name of the remote type */
} LogicalRepTyp;
```
## Detailed Description
This structure provides essential type mapping information for logical replication when custom or user-defined data types are involved. It allows the subscriber to understand and properly handle data types that originate from the publisher, which is particularly important when the publisher and subscriber may have different type definitions or when custom types are used.

The structure captures the remote type's identity and naming information, which can be used to map between remote and local type representations. This is crucial for ensuring data integrity and proper type conversion during the replication process.

## Parameters / Member Variables
- `remoteid`: Oid value representing the unique identifier of the data type on the remote (publisher) side
- `*nspname`: String containing the schema (namespace) name where the remote type is defined
- `*typname`: String containing the name of the remote data type
## Dependencies
- Functions called/Symbols referenced:
  - Oid (PostgreSQL object identifier type)
- Called from (representative examples):
  - [logicalrep_read_typ](../l/logicalrep_read_typ.md)
  - [apply_handle_type](../a/apply_handle_type.md)

## Notes and Other Information
- This structure is particularly important when dealing with custom or user-defined types in logical replication
- Type mapping ensures that data types are correctly interpreted and converted between publisher and subscriber
- The structure helps maintain data type consistency across different PostgreSQL instances
- Memory management for the string fields must be handled properly to avoid leaks
- This is a relatively simple structure compared to other logical replication structures, focusing specifically on type identification
- Located in src/include/replication/logicalproto.h:119-124