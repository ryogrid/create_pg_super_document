# PQchangePassword

## Location
[src/interfaces/libpq/fe-auth.c:1401-1454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-auth.c#L1401-L1454)

## Overview
High-level function that securely changes a PostgreSQL user's password by encrypting it client-side and executing an ALTER USER statement.

## Definition

```c
PGresult *
PQchangePassword(PGconn *conn, const char *user, const char *passwd)
```
## Detailed Description
 provides a secure mechanism for client applications to change PostgreSQL user passwords. The function ensures that plaintext passwords never reach the server by performing client-side encryption using the server's configured password encryption method.

The function orchestrates several security-critical operations:
1. Encrypts the password using  with the server's password_encryption setting
2. Properly escapes both the encrypted password and username to prevent SQL injection
3. Constructs and executes an "ALTER USER ... PASSWORD ..." SQL statement
4. Manages memory cleanup of all intermediate values

This approach ensures that cleartext passwords never appear in server logs, pg_stat displays, or other server-side monitoring systems. The encryption method is automatically determined by the server's configuration, providing transparency to the client application.

## Parameters / Member Variables
- : Active PostgreSQL connection object
- : SQL name of the target user whose password is being changed
- Changing password for ryo.: Cleartext password to be set (encrypted before transmission)

## Dependencies
- Functions called/Symbols referenced:
  - PQencryptPasswordConn (client-side password encryption)
  - [PQescapeLiteral](PQescapeLiteral.md) (SQL literal escaping for password)
  - [PQescapeIdentifier](PQescapeIdentifier.md) (SQL identifier escaping for username)
  - [PQexec](PQexec.md) (SQL statement execution)
  - [PQfreemem](PQfreemem.md) (memory management)
  - [initPQExpBuffer](../i/initPQExpBuffer.md), printfPQExpBuffer, termPQExpBuffer (buffer management)
- Called from (representative examples):
  - [exec_command_password](../e/exec_command_password.md) (psql password command implementation)

## Notes and Other Information
- Returns PGresult pointer that must be freed with PQclear() by caller
- Returns NULL on encryption, escaping, or memory allocation failures
- Use PQresultStatus() to check for SQL execution errors
- Use PQerrorMessage() for detailed error information
- Automatically uses server's password_encryption setting for encryption method
- Prevents SQL injection through proper escaping of user input
- Memory-safe: cleans up all intermediate allocations before returning
- Thread-safe when used with separate connection objects per thread
- Security feature: plaintext password never transmitted to server

## Simplified Source

```c
PGresult *
PQchangePassword(PGconn *conn, const char *user, const char *passwd)
{
    // Encrypt password using server's preferred method
    char *encrypted_password = PQencryptPasswordConn(conn, passwd, user, NULL);
    if (!encrypted_password) {
        return NULL;
    }

    // Escape the encrypted password for SQL
    char *fmtpw = PQescapeLiteral(conn, encrypted_password, strlen(encrypted_password));
    PQfreemem(encrypted_password);
    if (!fmtpw) {
        return NULL;
    }

    // Escape the username for SQL
    char *fmtuser = PQescapeIdentifier(conn, user, strlen(user));
    if (!fmtuser) {
        PQfreemem(fmtpw);
        return NULL;
    }

    // Build and execute ALTER USER statement
    PQExpBufferData buf;
    initPQExpBuffer(&buf);
    printfPQExpBuffer(&buf, "ALTER USER %s PASSWORD %s", fmtuser, fmtpw);

    PGresult *res = PQexec(conn, buf.data);

    // Clean up allocated memory
    termPQExpBuffer(&buf);
    PQfreemem(fmtuser);
    PQfreemem(fmtpw);

    return res;
}
```