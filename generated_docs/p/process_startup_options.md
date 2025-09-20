# process_startup_options

## Location
[src/backend/utils/init/postinit.c:1262-1326](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L1262-L1326)

## Overview
process_startup_options processes command-line switches and GUC variable settings passed in the client startup packet, applying them with appropriate security context based on user privileges.

## Definition

```c
static void
process_startup_options(Port *port, bool am_superuser)
```
## Detailed Description
process_startup_options is a static function responsible for processing configuration options sent by the client during connection startup. It handles two types of configuration: command-line switches embedded in the startup packet and explicit GUC variable settings.

The function first determines the appropriate GUC context based on whether the connected user is a superuser (PGC_SU_BACKEND) or a regular user (PGC_BACKEND). This context controls which configuration parameters can be modified by the client.

For command-line options, the function parses the cmdline_options string using pg_split_opts() to convert it into an argument vector, then processes these arguments through process_postgres_switches(). For GUC options, it iterates through name-value pairs in the guc_options list and applies each setting using SetConfigOption().

## Parameters / Member Variables
- : Pointer to the Port structure containing client connection information and startup packet data
- : Boolean flag indicating whether the connecting user has superuser privileges

## Dependencies
- Functions called/Symbols referenced:
  - [pg_split_opts](pg_split_opts.md)
  - [process_postgres_switches](process_postgres_switches.md)
  - list_head
  - [lnext](../l/lnext.md)
  - [SetConfigOption](../S/SetConfigOption.md)
  - PGC_SU_BACKEND
  - PGC_BACKEND
  - PGC_S_CLIENT
- Called from (representative examples):
  - [InitPostgres](../I/InitPostgres.md) (twice - for WAL senders and regular database connections)

## Notes and Other Information
- This is a static function, only callable within the same source file
- Security is enforced through GUC context levels - superusers can modify more parameters than regular users
- [Command](../C/Command.md)-line options are processed first, followed by explicit GUC settings
- The function uses palloc for memory allocation of the argument vector
- GUC options are processed as name-value pairs from a linked list structure
- All settings are applied with PGC_S_CLIENT source priority to indicate they came from the client
- This function is critical for allowing clients to customize their session configuration during connection establishment