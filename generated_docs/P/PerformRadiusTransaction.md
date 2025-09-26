# PerformRadiusTransaction

## Location
[src/backend/libpq/auth.c:2942-3261](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2942-L3261)

## Overview
Executes the complete RADIUS authentication protocol transaction including packet construction, network communication, and response validation.

## Definition

```c
struct sockaddr_in6 localaddr;
```
## Detailed Description
This static function implements the core RADIUS authentication protocol as defined in RFC 2865. It handles the complete transaction lifecycle including:

1. **Server Resolution**: Resolves the RADIUS server hostname to network addresses
2. **Packet Construction**: Builds a properly formatted RADIUS Access-Request packet with required attributes
3. **Password Encryption**: Implements RADIUS password encryption using MD5 and shared secret
4. **Network Communication**: Creates UDP socket, sends request, and listens for response
5. **Response Validation**: Validates response authenticity using MD5 signature verification
6. **Timeout Handling**: Implements robust timeout mechanism with retry capability

The function uses UDP sockets for communication and implements the RADIUS protocol's cryptographic requirements including:
- Random vector generation for Request Authenticator
- Password encryption using MD5(secret + Request Authenticator) XOR methodology
- Response Authenticator validation using MD5(Code+ID+Length+RequestAuthenticator+Attributes+Secret)

Key security features include protection against denial-of-service attacks by validating packet sources and ignoring invalid packets while continuing to wait for valid responses.

## Parameters / Member Variables
- : RADIUS server hostname or IP address to connect to
- : Shared secret string used for packet authentication and password encryption
- : Port number as string (defaults to "1812" if NULL)
- : NAS (Network Access Server) identifier string (defaults to "postgresql" if NULL)  
- : Username to authenticate
- Changing password for ryo.: Password to authenticate (will be encrypted before transmission)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_getaddrinfo_all](../p/pg_getaddrinfo_all.md), pg_freeaddrinfo_all (address resolution)
  - [pg_strong_random](../p/pg_strong_random.md) (cryptographic random number generation)
  - [pg_md5_binary](../p/pg_md5_binary.md) (MD5 hashing for authentication)
  - pg_hton32, pg_hton16, pg_ntoh16 (network byte order conversion)
  - [radius_add_attribute](../r/radius_add_attribute.md) (adding attributes to RADIUS packet)
  - socket, bind, sendto, recvfrom, closesocket (network operations)
  - select, gettimeofday (timeout and I/O multiplexing)
  - ereport, errmsg (error logging)
  - [palloc](../p/palloc.md), pfree (memory management)
- Constants referenced:
  - RADIUS_ACCESS_REQUEST, RADIUS_ACCESS_ACCEPT, RADIUS_ACCESS_REJECT
  - RADIUS_AUTHENTICATE_ONLY, RADIUS_SERVICE_TYPE, RADIUS_USER_NAME, RADIUS_NAS_IDENTIFIER, RADIUS_PASSWORD
  - RADIUS_HEADER_LENGTH, RADIUS_VECTOR_LENGTH, RADIUS_BUFFER_SIZE, RADIUS_MAX_PASSWORD_LENGTH, RADIUS_TIMEOUT
  - STATUS_OK, STATUS_ERROR, STATUS_EOF
- Types referenced:
  - radius_packet, pgsocket, socklen_t
- Called from:
  - [CheckRADIUSAuth](../C/CheckRADIUSAuth.md) at src/backend/libpq/auth.c:2896

## Notes and Other Information
- This is a static function, only visible within the auth.c compilation unit
- Implements RFC 2865 RADIUS authentication protocol with full cryptographic verification
- Uses UDP as transport protocol (standard for RADIUS)
- Supports both IPv4 and IPv6 server addresses
- Password is encrypted using iterative MD5 XOR method in 16-byte blocks
- Implements robust packet validation to prevent spoofing and denial-of-service attacks
- Default timeout is defined by RADIUS_TIMEOUT constant
- Returns STATUS_OK for successful authentication, STATUS_EOF for explicit rejection, STATUS_ERROR for errors
- Memory cleanup is performed for all allocated resources regardless of success/failure
- Handles network interrupts gracefully and continues operation when appropriate
- Part of PostgreSQL's external authentication infrastructure supporting RADIUS servers