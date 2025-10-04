# libpqrcv_startstreaming

## Location
[src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:551-654](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/libpqwalreceiver/libpqwalreceiver.c#L551-L654)

## Overview
Initiates WAL (Write-Ahead Log) streaming from a PostgreSQL primary server using specified streaming options, supporting both physical and logical replication modes.

## Definition

```c
static bool
libpqrcv_startstreaming(WalReceiverConn *conn,
						const WalRcvStreamOptions *options)
```
## Detailed Description
This function constructs and executes a START_REPLICATION command to begin streaming WAL data from a PostgreSQL primary server. It supports both physical and logical replication modes, with different parameter handling for each. For logical replication, it processes complex options like publication names, protocol version, streaming mode, two-phase commit support, and binary format. For physical replication, it specifies the timeline ID. 

The function performs version-specific feature checking, ensuring that advanced features like two-phase commit and origin tracking are only used with compatible server versions. It handles proper string escaping and memory management throughout the command construction process.

## Parameters / Member Variables
- `*conn`: Pointer to WalReceiverConn structure containing the connection to the primary server
- `*options`: Pointer to WalRcvStreamOptions structure containing all streaming configuration parameters including start point, slot name, and replication mode-specific settings
## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md)
  - [appendStringInfoString](../a/appendStringInfoString.md)
  - [appendStringInfo](../a/appendStringInfo.md)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)
  - [stringlist_to_identifierstr](../s/stringlist_to_identifierstr.md)
  - [PQescapeLiteral](../P/PQescapeLiteral.md)
  - [PQfreemem](../P/PQfreemem.md)
  - [PQserverVersion](../P/PQserverVersion.md)
  - [libpqrcv_PQexec](libpqrcv_PQexec.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [pchomp](../p/pchomp.md)
  - [pfree](../p/pfree.md)
- Called from (representative examples):
  - [WalReceiverConn](../W/WalReceiverConn.md) (referenced in streaming setup routines)

## Dependencies
- Functions called/Symbols referenced:
  - LSN_FORMAT_ARGS (macro for formatting LSN)
  - PGRES_COMMAND_OK
  - PGRES_COPY_BOTH

## Notes and Other Information
- This is a static function internal to the libpqwalreceiver module
- Returns true if successfully switched to copy-both mode (streaming active), false if command succeeded but no WAL available
- Throws ERROR on failure conditions including protocol violations and memory allocation errors
- Supports server version compatibility checks for features like two-phase commit (15.0+), origin tracking (16.0+), and binary format (14.0+)
- Handles both slot-based and slotless replication scenarios
- Properly escapes publication names to prevent SQL injection in logical replication
- Located at src/backend/replication/libpqwalreceiver/libpqwalreceiver.c:551-654

## Simplified Source

```c
static bool
libpqrcv_startstreaming(WalReceiverConn *conn, const WalRcvStreamOptions *options)
{
    StringInfoData cmd;
    PGresult *res;

    // Validate connection type matches options
    Assert(options->logical == conn->logical);
    Assert(options->slotname || !options->logical);

    initStringInfo(&cmd);

    // Build START_REPLICATION command
    appendStringInfoString(&cmd, "START_REPLICATION");
    if (options->slotname != NULL)
        appendStringInfo(&cmd, " SLOT \"%s\"", options->slotname);

    if (options->logical)
        appendStringInfoString(&cmd, " LOGICAL");

    appendStringInfo(&cmd, " %X/%X", LSN_FORMAT_ARGS(options->startpoint));

    // Add replication-specific options
    if (options->logical) {
        // Logical replication: protocol version, streaming, two-phase, etc.
        appendStringInfoString(&cmd, " (");
        appendStringInfo(&cmd, "proto_version '%u'", options->proto.logical.proto_version);

        if (options->proto.logical.streaming_str)
            appendStringInfo(&cmd, ", streaming '%s'", options->proto.logical.streaming_str);

        // Version-specific features
        if (options->proto.logical.twophase && PQserverVersion(conn->streamConn) >= 150000)
            appendStringInfoString(&cmd, ", two_phase 'on'");

        if (options->proto.logical.origin && PQserverVersion(conn->streamConn) >= 160000)
            appendStringInfo(&cmd, ", origin '%s'", options->proto.logical.origin);

        // Handle publication names with proper escaping
        char *pubnames_str = stringlist_to_identifierstr(conn->streamConn, options->proto.logical.publication_names);
        if (!pubnames_str)
            ereport(ERROR, (errmsg("could not start WAL streaming: %s", pchomp(PQerrorMessage(conn->streamConn)))));

        char *pubnames_literal = PQescapeLiteral(conn->streamConn, pubnames_str, strlen(pubnames_str));
        if (!pubnames_literal)
            ereport(ERROR, (errmsg("could not start WAL streaming: %s", pchomp(PQerrorMessage(conn->streamConn)))));

        appendStringInfo(&cmd, ", publication_names %s", pubnames_literal);
        PQfreemem(pubnames_literal);
        pfree(pubnames_str);

        if (options->proto.logical.binary && PQserverVersion(conn->streamConn) >= 140000)
            appendStringInfoString(&cmd, ", binary 'true'");

        appendStringInfoChar(&cmd, ')');
    } else {
        // Physical replication: timeline only
        appendStringInfo(&cmd, " TIMELINE %u", options->proto.physical.startpointTLI);
    }

    // Execute streaming command
    res = libpqrcv_PQexec(conn->streamConn, cmd.data);
    pfree(cmd.data);

    // Handle result
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
        PQclear(res);
        return false;  // No WAL available at requested point
    } else if (PQresultStatus(res) != PGRES_COPY_BOTH) {
        PQclear(res);
        ereport(ERROR, (errmsg("could not start WAL streaming: %s", pchomp(PQerrorMessage(conn->streamConn)))));
    }

    PQclear(res);
    return true;  // Successfully started streaming
}
```