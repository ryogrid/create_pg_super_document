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

## Simplified Source

```c
static int
PerformRadiusTransaction(const char *server, const char *secret, const char *portstr,
                         const char *identifier, const char *user_name, const char *passwd)
{
    radius_packet radius_send_pack;
    radius_packet radius_recv_pack;
    radius_packet *packet = &radius_send_pack;
    radius_packet *receivepacket = &radius_recv_pack;
    char *radius_buffer = (char *) &radius_send_pack;
    char *receive_buffer = (char *) &radius_recv_pack;

    uint8 encryptedpassword[RADIUS_MAX_PASSWORD_LENGTH];
    uint8 *cryptvector;
    int32 service = pg_hton32(RADIUS_AUTHENTICATE_ONLY);
    int encryptedpasswordlen;
    int packetlength;
    pgsocket sock;

    struct addrinfo hint;
    struct addrinfo *serveraddrs;
    struct sockaddr_in6 localaddr;
    struct sockaddr_in6 remoteaddr;
    struct timeval endtime;
    fd_set fdset;
    int port, r;
    socklen_t addrsize;

    // Set default values
    if (portstr == NULL) portstr = "1812";
    if (identifier == NULL) identifier = "postgresql";

    // Resolve server address
    MemSet(&hint, 0, sizeof(hint));
    hint.ai_socktype = SOCK_DGRAM;
    hint.ai_family = AF_UNSPEC;
    port = atoi(portstr);

    r = pg_getaddrinfo_all(server, portstr, &hint, &serveraddrs);
    if (r || !serveraddrs)
    {
        ereport(LOG, (errmsg("could not translate RADIUS server name \"%s\" to address", server)));
        if (serveraddrs) pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    // Construct RADIUS Access-Request packet
    packet->code = RADIUS_ACCESS_REQUEST;
    packet->length = RADIUS_HEADER_LENGTH;

    // Generate random vector for authentication
    if (!pg_strong_random(packet->vector, RADIUS_VECTOR_LENGTH))
    {
        ereport(LOG, (errmsg("could not generate random encryption vector")));
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }
    packet->id = packet->vector[0];

    // Add RADIUS attributes
    radius_add_attribute(packet, RADIUS_SERVICE_TYPE, (const unsigned char *) &service, sizeof(service));
    radius_add_attribute(packet, RADIUS_USER_NAME, (const unsigned char *) user_name, strlen(user_name));
    radius_add_attribute(packet, RADIUS_NAS_IDENTIFIER, (const unsigned char *) identifier, strlen(identifier));

    // Encrypt password using RADIUS method: e[i] = p[i] XOR MD5(secret + vector)
    encryptedpasswordlen = ((strlen(passwd) + RADIUS_VECTOR_LENGTH - 1) / RADIUS_VECTOR_LENGTH) * RADIUS_VECTOR_LENGTH;
    cryptvector = palloc(strlen(secret) + RADIUS_VECTOR_LENGTH);
    memcpy(cryptvector, secret, strlen(secret));

    uint8 *md5trailer = packet->vector;
    for (int i = 0; i < encryptedpasswordlen; i += RADIUS_VECTOR_LENGTH)
    {
        const char *errstr = NULL;

        memcpy(cryptvector + strlen(secret), md5trailer, RADIUS_VECTOR_LENGTH);

        if (!pg_md5_binary(cryptvector, strlen(secret) + RADIUS_VECTOR_LENGTH,
                           encryptedpassword + i, &errstr))
        {
            ereport(LOG, (errmsg("could not perform MD5 encryption of password: %s", errstr)));
            pfree(cryptvector);
            pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
            return STATUS_ERROR;
        }

        // XOR password bytes with MD5 hash
        for (int j = i; j < i + RADIUS_VECTOR_LENGTH; j++)
        {
            if (j < strlen(passwd))
                encryptedpassword[j] = passwd[j] ^ encryptedpassword[j];
            else
                encryptedpassword[j] = '\0' ^ encryptedpassword[j];
        }

        md5trailer = encryptedpassword + i;
    }
    pfree(cryptvector);

    radius_add_attribute(packet, RADIUS_PASSWORD, encryptedpassword, encryptedpasswordlen);

    // Create UDP socket and send packet
    packetlength = packet->length;
    packet->length = pg_hton16(packet->length);

    sock = socket(serveraddrs[0].ai_family, SOCK_DGRAM, 0);
    if (sock == PGINVALID_SOCKET)
    {
        ereport(LOG, (errmsg("could not create RADIUS socket: %m")));
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    // Bind and send request
    memset(&localaddr, 0, sizeof(localaddr));
    localaddr.sin6_family = serveraddrs[0].ai_family;
    localaddr.sin6_addr = in6addr_any;
    addrsize = (localaddr.sin6_family == AF_INET6) ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

    if (bind(sock, (struct sockaddr *) &localaddr, addrsize) ||
        sendto(sock, radius_buffer, packetlength, 0, serveraddrs[0].ai_addr, serveraddrs[0].ai_addrlen) < 0)
    {
        ereport(LOG, (errmsg("could not send RADIUS packet: %m")));
        closesocket(sock);
        pg_freeaddrinfo_all(hint.ai_family, serveraddrs);
        return STATUS_ERROR;
    }

    pg_freeaddrinfo_all(hint.ai_family, serveraddrs);

    // Wait for response with timeout
    gettimeofday(&endtime, NULL);
    endtime.tv_sec += RADIUS_TIMEOUT;

    while (true)
    {
        struct timeval timeout, now;
        int64 timeoutval;

        gettimeofday(&now, NULL);
        timeoutval = (endtime.tv_sec * 1000000 + endtime.tv_usec) - (now.tv_sec * 1000000 + now.tv_usec);

        if (timeoutval <= 0)
        {
            ereport(LOG, (errmsg("timeout waiting for RADIUS response from %s", server)));
            closesocket(sock);
            return STATUS_ERROR;
        }

        timeout.tv_sec = timeoutval / 1000000;
        timeout.tv_usec = timeoutval % 1000000;

        FD_ZERO(&fdset);
        FD_SET(sock, &fdset);

        r = select(sock + 1, &fdset, NULL, NULL, &timeout);
        if (r < 0)
        {
            if (errno == EINTR) continue;
            ereport(LOG, (errmsg("could not check status on RADIUS socket: %m")));
            closesocket(sock);
            return STATUS_ERROR;
        }
        if (r == 0)
        {
            ereport(LOG, (errmsg("timeout waiting for RADIUS response from %s", server)));
            closesocket(sock);
            return STATUS_ERROR;
        }

        // Receive and validate response
        addrsize = sizeof(remoteaddr);
        packetlength = recvfrom(sock, receive_buffer, RADIUS_BUFFER_SIZE, 0,
                                (struct sockaddr *) &remoteaddr, &addrsize);

        if (packetlength < 0)
        {
            ereport(LOG, (errmsg("could not read RADIUS response: %m")));
            closesocket(sock);
            return STATUS_ERROR;
        }

        // Validate response packet format and source
        if (remoteaddr.sin6_port != pg_hton16(port) ||
            packetlength < RADIUS_HEADER_LENGTH ||
            packetlength != pg_ntoh16(receivepacket->length) ||
            packet->id != receivepacket->id)
        {
            continue; // Invalid packet, wait for another
        }

        // Verify response authenticator
        cryptvector = palloc(packetlength + strlen(secret));
        memcpy(cryptvector, receivepacket, 4); // code+id+length
        memcpy(cryptvector + 4, packet->vector, RADIUS_VECTOR_LENGTH); // request authenticator
        if (packetlength > RADIUS_HEADER_LENGTH)
            memcpy(cryptvector + RADIUS_HEADER_LENGTH, receive_buffer + RADIUS_HEADER_LENGTH,
                   packetlength - RADIUS_HEADER_LENGTH);
        memcpy(cryptvector + packetlength, secret, strlen(secret));

        const char *errstr = NULL;
        if (!pg_md5_binary(cryptvector, packetlength + strlen(secret), encryptedpassword, &errstr))
        {
            ereport(LOG, (errmsg("could not perform MD5 encryption of received packet: %s", errstr)));
            pfree(cryptvector);
            continue;
        }
        pfree(cryptvector);

        // Verify MD5 signature
        if (memcmp(receivepacket->vector, encryptedpassword, RADIUS_VECTOR_LENGTH) != 0)
        {
            ereport(LOG, (errmsg("RADIUS response from %s has incorrect MD5 signature", server)));
            continue;
        }

        // Process response code
        if (receivepacket->code == RADIUS_ACCESS_ACCEPT)
        {
            closesocket(sock);
            return STATUS_OK;
        }
        else if (receivepacket->code == RADIUS_ACCESS_REJECT)
        {
            closesocket(sock);
            return STATUS_EOF;
        }
        else
        {
            ereport(LOG, (errmsg("RADIUS response from %s has invalid code (%d) for user \"%s\"",
                                 server, receivepacket->code, user_name)));
            continue;
        }
    }
}
```