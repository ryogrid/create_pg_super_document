# Port

## Location
[src/include/libpq/libpq-be.h:132-232](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/libpq-be.h#L132-L232)

## Overview
Port is a comprehensive structure that holds all state information about a client connection in a PostgreSQL backend process, available globally as MyProcPort.

## Definition
```c
typedef struct Port
{
    pgsocket    sock;                    /* File descriptor */
    bool        noblock;                 /* is the socket in non-blocking mode? */
    ProtocolVersion proto;               /* FE/BE protocol version */
    SockAddr    laddr;                   /* local addr (postmaster) */
    SockAddr    raddr;                   /* remote addr (client) */
    char       *remote_host;             /* name (or ip addr) of remote host */
    char       *remote_hostname;         /* name (not ip addr) of remote host, if available */
    int         remote_hostname_resolv;  /* hostname verification state */
    int         remote_hostname_errcode; /* DNS lookup error code */
    char       *remote_port;             /* text rep of remote port */
    
    /* Startup packet information */
    char       *database_name;
    char       *user_name;
    char       *cmdline_options;
    List       *guc_options;
    char       *application_name;
    
    /* Authentication cycle information */
    HbaLine    *hba;
    
    /* TCP keepalive and user timeout settings */
    int         default_keepalives_idle;
    int         default_keepalives_interval;
    int         default_keepalives_count;
    int         default_tcp_user_timeout;
    int         keepalives_idle;
    int         keepalives_interval;
    int         keepalives_count;
    int         tcp_user_timeout;
    
    /* GSSAPI structures */
    pg_gssinfo *gss;
    
    /* SSL structures */
    bool        ssl_in_use;
    char       *peer_cn;
    char       *peer_dn;
    bool        peer_cert_valid;
    bool        alpn_used;
    SSL        *ssl;
    X509       *peer;
    
    /* Raw buffer for SSL establishment */
    char       *raw_buf;
    ssize_t     raw_buf_consumed;
    ssize_t     raw_buf_remaining;
} Port;
```

## Detailed Description
Port serves as the central repository for all connection-related state in a PostgreSQL backend process. It encompasses network socket information, client identity data, authentication state, security protocol details, and connection parameters. The structure is designed to persist throughout the entire lifetime of a client connection and all its data is maintained in TopMemoryContext.

The structure handles complex hostname resolution logic, tracking whether reverse and forward DNS lookups have been performed and their results. It also manages authentication state during the HBA (Host-Based Authentication) process and maintains SSL/TLS and GSSAPI security context information when encryption is enabled.

## Parameters / Member Variables
- `sock`: The underlying socket file descriptor for the client connection
- `noblock`: Boolean indicating whether the socket operates in non-blocking mode
- `proto`: The protocol version negotiated between frontend and backend
- `laddr`: Local socket address (postmaster side)
- `raddr`: Remote socket address (client side)
- `remote_host`: Client hostname or IP address as string
- `remote_hostname`: Resolved hostname (not IP) if DNS lookup succeeded
- `remote_hostname_resolv`: Hostname verification state (+1=verified, -1=failed, 0=pending, -2=error)
- `remote_hostname_errcode`: Error code from failed DNS lookups for gai_strerror()
- `remote_port`: Client port number as text
- `database_name`: Target database name from startup packet
- `user_name`: Connecting username from startup packet
- `cmdline_options`: Command line options passed in startup packet
- `guc_options`: List of GUC parameter name/value pairs from startup packet
- `application_name`: Application name from startup packet (for logging only)
- `hba`: HBA line that matches this connection during authentication
- `default_keepalives_idle`, `default_keepalives_interval`, `default_keepalives_count`, `default_tcp_user_timeout`: Default TCP keepalive parameter values
- `keepalives_idle`, `keepalives_interval`, `keepalives_count`: Current TCP keepalive parameter values
- `tcp_user_timeout`: TCP user timeout setting
- `gss`: GSSAPI authentication and encryption context
- `ssl_in_use`: Boolean indicating SSL/TLS is active
- `peer_cn`: SSL peer certificate common name
- `peer_dn`: SSL peer certificate distinguished name
- `peer_cert_valid`: Boolean indicating peer certificate validation status
- `alpn_used`: Boolean indicating ALPN (Application Layer Protocol Negotiation) usage
- `ssl`: OpenSSL connection context
- `peer`: Peer X.509 certificate
- `raw_buf`: Buffer for data that needs to be "unread" during SSL establishment
- `raw_buf_consumed`: Bytes already consumed from raw_buf
- `raw_buf_remaining`: Bytes remaining in raw_buf

## Dependencies
- Functions called/Symbols referenced:
  - pgsocket (socket type)
  - ProtocolVersion (protocol version enum)
  - [SockAddr](../S/SockAddr.md) (socket address structure)
  - [HbaLine](../H/HbaLine.md) (host-based authentication configuration)
  - pg_gssinfo (GSSAPI context)
  - ssize_t (signed size type)
- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md) (in src/backend/tcop/backend_startup.c:126)
  - [ClientAuthentication](../C/ClientAuthentication.md) (in src/backend/libpq/auth.c:390)
  - [secure_open_server](../s/secure_open_server.md) (in src/backend/libpq/be-secure.c:110)
  - [ProcessStartupPacket](ProcessStartupPacket.md) (in src/backend/tcop/backend_startup.c:453)

## Notes and Other Information
- All data pointed to by Port structure must be allocated in TopMemoryContext
- Hostname resolution follows a specific state machine tracked by remote_hostname_resolv
- SSL/TLS field offsets depend on USE_OPENSSL compilation flag, affecting ABI compatibility
- The raw_buf mechanism provides a workaround for SSL layer "unread" requirements
- GSSAPI support is conditional on ENABLE_GSS or ENABLE_SSPI compilation flags
- Used extensively throughout the authentication, security, and communication subsystems
- Available globally as MyProcPort variable in backend processes