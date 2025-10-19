# appendPsqlMetaConnect

## Location
[src/fe_utils/string_utils.c:743-818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L743-L818)

## Overview
Appends a psql meta-command that connects to the given database using the current connection's user, host, and port parameters.

## Definition
```c
void appendPsqlMetaConnect(PQExpBuffer buf, const char *dbname)
```

## Detailed Description
This function generates a psql meta-command to connect to a specified database. It analyzes the database name to determine the appropriate connection syntax:

1. For simple ASCII names (containing only letters, digits, underscores, and periods), it generates a simple `\connect` command
2. For complex names (containing special characters), it uses the more robust `\connect -reuse-previous=on` format with proper connection string encoding

The function ensures proper handling of special characters by:
- Checking for invalid characters like newlines/carriage returns (causes program exit)
- Using SQL_ASCII encoding for complex database names
- Applying proper identifier quoting through `fmtIdEnc`

## Parameters / Member Variables
- `buf`: PQExpBuffer to append the meta-command to
- `dbname`: The target database name to connect to

## Dependencies
- Functions called/Symbols referenced:
  - [initPQExpBuffer](../i/initPQExpBuffer.md)
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)
  - [appendConnStrVal](appendConnStrVal.md)  
  - [fmtIdEnc](../f/fmtIdEnc.md)
  - [termPQExpBuffer](../t/termPQExpBuffer.md)
  - [appendPQExpBufferChar](appendPQExpBufferChar.md)
  - PG_SQL_ASCII
  - EXIT_FAILURE
  - [PQExpBufferData](../P/PQExpBufferData.md)

- Called from (representative examples):
  - [_reconnectToDB](../r/_reconnectToDB.md) (src/bin/pg_dump/pg_backup_archiver.c:3382)
  - [old_9_6_invalidate_hash_indexes](../o/old_9_6_invalidate_hash_indexes.md) (src/bin/pg_upgrade/version.c:85)
  - [report_extension_updates](../r/report_extension_updates.md) (src/bin/pg_upgrade/version.c:183)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:743-818
- Exits with EXIT_FAILURE if database name contains newline or carriage return characters
- Forces SQL_ASCII encoding for complex database names to ensure proper forwarding to server
- Uses different quoting strategies based on database name complexity to maintain compatibility with different PostgreSQL versions
- Part of the frontend utilities library for database connection management

## Simplified Source

```c
void appendPsqlMetaConnect(PQExpBuffer buf, const char *dbname) {
    const char *s;
    bool complex;

    // Check for dangerous characters and complexity
    complex = false;
    for (s = dbname; *s; s++) {
        if (*s == '\n' || *s == '\r') {
            // Fatal error for newlines/carriage returns
            fprintf(stderr,
                    _("database name contains a newline or carriage return: \"%s\"\n"),
                    dbname);
            exit(EXIT_FAILURE);
        }

        if (!((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') ||
              (*s >= '0' && *s <= '9') || *s == '_' || *s == '.')) {
            complex = true;
        }
    }

    if (complex) {
        // Complex database name - use connection string format
        PQExpBufferData connstr;
        initPQExpBuffer(&connstr);

        // Force SQL_ASCII encoding for proper forwarding
        appendPQExpBufferStr(buf, "\\encoding SQL_ASCII\n");
        appendPQExpBufferStr(buf, "\\connect -reuse-previous=on ");

        // Build connection string with proper escaping
        appendPQExpBufferStr(&connstr, "dbname=");
        appendConnStrVal(&connstr, dbname);

        // Use SQL identifier quoting for meta-command parser
        appendPQExpBufferStr(buf, fmtIdEnc(connstr.data, PG_SQL_ASCII));

        termPQExpBuffer(&connstr);
    } else {
        // Simple ASCII name - use basic connect command
        appendPQExpBufferStr(buf, "\\connect ");
        appendPQExpBufferStr(buf, fmtIdEnc(dbname, PG_SQL_ASCII));
    }

    appendPQExpBufferChar(buf, '\n');
}
```