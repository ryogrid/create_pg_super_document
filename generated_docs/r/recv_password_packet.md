# recv_password_packet

## Location
src/backend/libpq/auth.c: 714 - 794

## Overview
Receives and validates password response packets from the frontend client during password-based authentication methods.

## Definition


## Detailed Description
The  function handles the reception and basic validation of password messages sent by clients during authentication. It implements PostgreSQL's password message protocol, which expects a specific message format containing the password data.

The function performs several critical validations to ensure protocol compliance and security. It verifies that the received message is indeed a password message (type 'p'), validates the packet structure by comparing the message length with the actual string length, and enforces a policy against empty passwords.

The function is designed to handle common client behaviors gracefully, such as when clients disconnect without providing a password (common with psql), and provides appropriate error handling without logging sensitive password data.

One important security feature is that the actual password content is never logged - only a generic "received password packet" debug message is recorded to prevent sensitive data from appearing in server logs.

## Parameters / Member Variables
- : Pointer to Port structure containing connection information

## Dependencies
- Functions called/Symbols referenced:
  - [pq_startmsgread](../p/pq_startmsgread.md) (start reading a protocol message)
  - [pq_getbyte](../p/pq_getbyte.md) (read a single byte from client)
  - pq_getmessage (read the complete message content)
  - initStringInfo (initialize string buffer)
  - [pfree](../p/pfree.md) (free allocated memory)
  - ereport (send error messages)
  - elog (debug logging)
- Constants used:
  - PqMsg_PasswordMessage (expected message type identifier)
  - PG_MAX_AUTH_TOKEN_LENGTH (maximum allowed password length)
  - ERRCODE_PROTOCOL_VIOLATION (protocol error code)
  - ERRCODE_INVALID_PASSWORD (password validation error code)
  - DEBUG5 (logging level)
- Called from (representative examples):
  - [CheckPasswordAuth](../C/CheckPasswordAuth.md) (plaintext password authentication)
  - [CheckMD5Auth](../C/CheckMD5Auth.md) (MD5 password authentication)
  - [pam_passwd_conv_proc](../p/pam_passwd_conv_proc.md) (PAM authentication)
  - [CheckBSDAuth](../C/CheckBSDAuth.md) (BSD authentication)
  - [CheckLDAPAuth](../C/CheckLDAPAuth.md) (LDAP authentication)
  - [CheckRADIUSAuth](../C/CheckRADIUSAuth.md) (RADIUS authentication)

## Notes and Other Information
- Returns NULL on failure (EOF or invalid message), otherwise returns palloc'd string containing the password
- The function never echoes passwords to logs for security reasons
- Empty passwords are explicitly rejected to prevent confusion with "no password" authentication
- Length validation prevents potential buffer overflow or protocol violations
- Handles client disconnection gracefully without generating log spam (common with psql)
- No character set conversion is performed since client encoding is not yet established
- The returned password string must be freed by the caller using pfree()
- Part of PostgreSQL's broader password authentication infrastructure supporting multiple external authentication systems