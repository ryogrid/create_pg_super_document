# libpqrcv_alter_slot

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1123-1149](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L1123-L1149)

## Overview
libpqrcv_alter_slot modifies the properties of an existing replication slot on the primary server, specifically changing the failover setting to enable or disable the slot's eligibility for failover scenarios.

## Definition
```c
static void libpqrcv_alter_slot(WalReceiverConn *conn, const char *slotname, bool failover)
```

## Detailed Description
This function constructs and executes an ALTER_REPLICATION_SLOT command to modify the failover property of an existing replication slot. The function builds a properly formatted SQL command using the slot name (which is quoted using quote_identifier for safety) and sets the FAILOVER option to either true or false based on the provided parameter. The command is executed through the streaming replication protocol, and the function performs proper error checking to ensure the alteration succeeds. This capability is essential for managing replication slot behavior in high-availability PostgreSQL deployments where slots may need to be promoted or demoted for failover purposes.

## Parameters / Member Variables
- `conn`: Pointer to WalReceiverConn structure containing the streaming connection to the primary server
- `slotname`: Name of the existing replication slot to modify (will be properly quoted in the command)
- `failover`: Boolean flag to set the slot's failover eligibility (true to enable, false to disable)

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo/appendStringInfo: PostgreSQL string manipulation functions for building the command
  - [quote_identifier](../q/quote_identifier.md): PostgreSQL function to safely quote SQL identifiers
  - [libpqrcv_PQexec](libpqrcv_PQexec.md): Internal wrapper for PQexec with connection management
  - [PQresultStatus](../P/PQresultStatus.md): libpq function to check the execution result status
  - [PQclear](../P/PQclear.md): libpq function to free result memory
  - [pchomp](../p/pchomp.md): PostgreSQL utility function to clean up error message strings
  - [pfree](../p/pfree.md): PostgreSQL memory management function
- Called from (representative examples):
  - Referenced by WalReceiverConn structure function pointers
  - Used by replication management code for slot configuration changes

## Notes and Other Information
- This is a static function, accessible only within the libpqwalreceiver.c compilation unit
- The function expects PGRES_COMMAND_OK as the success status (not PGRES_TUPLES_OK like create_slot)
- Proper SQL injection protection through quote_identifier usage
- Error handling follows PostgreSQL conventions with ereport() for command failures
- The ALTER_REPLICATION_SLOT command requires appropriate privileges on the primary server
- This functionality is typically used in high-availability setups where slots need dynamic reconfiguration
- Memory is properly managed with pfree() for the command string and PQclear() for the result
- Location: src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:1123-1149