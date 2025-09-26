# pg_conn

## Location
src/interfaces/libpq/libpq-int.h: 365 - 650

## Overview
The core structure that stores all state data associated with a single PostgreSQL database connection, encompassing connection parameters, authentication state, I/O buffers, and protocol management.

## Definition

```c
struct pg_conn
{
	/* Saved values of connection options */
	char	   *pghost;			/* the machine on which the server is running,
								 * or a path to a UNIX-domain socket, or a
								 * comma-separated list of machines and/or
								 * paths; if NULL, use DEFAULT_PGSOCKET_DIR */
	char	   *pghostaddr;		/* the numeric IP address of the machine on
								 * which the server is running, or a
								 * comma-separated list of same.  Takes
								 * precedence over pghost. */
	char	   *pgport;			/* the server's communication port number, or
								 * a comma-separated list of ports */
	char	   *connect_timeout;	/* connection timeout (numeric string) */
	char	   *pgtcp_user_timeout; /* tcp user timeout (numeric string) */
	char	   *client_encoding_initial;	/* encoding to use */
	char	   *pgoptions;		/* options to start the backend with */
	char	   *appname;		/* application name */
	char	   *fbappname;		/* fallback application name */
	char	   *dbName;			/* database name */
	char	   *replication;	/* connect as the replication standby? */
	char	   *pguser;			/* Postgres username and password, if any */
	char	   *pgpass;
	char	   *pgpassfile;		/* path to a file containing password(s) */
	char	   *channel_binding;	/* channel binding mode
									 * (require,prefer,disable) */
	char	   *keepalives;		/* use TCP keepalives? */
	char	   *keepalives_idle;	/* time between TCP keepalives */
	char	   *keepalives_interval;	/* time between TCP keepalive
										 * retransmits */
	char	   *keepalives_count;	/* maximum number of TCP keepalive
									 * retransmits */
	char	   *sslmode;		/* SSL mode (require,prefer,allow,disable) */
	char	   *sslnegotiation; /* SSL initiation style (postgres,direct) */
	char	   *sslcompression; /* SSL compression (0 or 1) */
	char	   *sslkey;			/* client key filename */
	char	   *sslcert;		/* client certificate filename */
	char	   *sslpassword;	/* client key file password */
	char	   *sslcertmode;	/* client cert mode (require,allow,disable) */
	char	   *sslrootcert;	/* root certificate filename */
	char	   *sslcrl;			/* certificate revocation list filename */
	char	   *sslcrldir;		/* certificate revocation list directory name */
	char	   *sslsni;			/* use SSL SNI extension (0 or 1) */
	char	   *requirepeer;	/* required peer credentials for local sockets */
	char	   *gssencmode;		/* GSS mode (require,prefer,disable) */
	char	   *krbsrvname;		/* Kerberos service name */
	char	   *gsslib;			/* What GSS library to use ("gssapi" or
								 * "sspi") */
	char	   *gssdelegation;	/* Try to delegate GSS credentials? (0 or 1) */
	char	   *ssl_min_protocol_version;	/* minimum TLS protocol version */
	char	   *ssl_max_protocol_version;	/* maximum TLS protocol version */
	char	   *target_session_attrs;	/* desired session properties */
	char	   *require_auth;	/* name of the expected auth method */
	char	   *load_balance_hosts; /* load balance over hosts */

	bool		cancelRequest;	/* true if this connection is used to send a
								 * cancel request, instead of being a normal
								 * connection that's used for queries */

	/* Optional file to write trace info to */
	FILE	   *Pfdebug;
	int			traceFlags;

	/* Callback procedures for notice message processing */
	PGNoticeHooks noticeHooks;

	/* Event procs registered via PQregisterEventProc */
	PGEvent    *events;			/* expandable array of event data */
	int			nEvents;		/* number of active events */
	int			eventArraySize; /* allocated array size */

	/* Status indicators */
	ConnStatusType status;
	PGAsyncStatusType asyncStatus;
	PGTransactionStatusType xactStatus; /* never changes to ACTIVE */
	char		last_sqlstate[6];	/* last reported SQLSTATE */
	bool		options_valid;	/* true if OK to attempt connection */
	bool		nonblocking;	/* whether this connection is using nonblock
								 * sending semantics */
	PGpipelineStatus pipelineStatus;	/* status of pipeline mode */
	bool		partialResMode; /* true if single-row or chunked mode */
	bool		singleRowMode;	/* return current query result row-by-row? */
	int			maxChunkSize;	/* return query result in chunks not exceeding
								 * this number of rows */
	char		copy_is_binary; /* 1 = copy binary, 0 = copy text */
	int			copy_already_done;	/* # bytes already returned in COPY OUT */
	PGnotify   *notifyHead;		/* oldest unreported Notify msg */
	PGnotify   *notifyTail;		/* newest unreported Notify msg */

	/* Support for multiple hosts in connection string */
	int			nconnhost;		/* # of hosts named in conn string */
	int			whichhost;		/* host we're currently trying/connected to */
	pg_conn_host *connhost;		/* details about each named host */
	char	   *connip;			/* IP address for current network connection */

	/*
	 * The pending command queue as a singly-linked list.  Head is the command
	 * currently in execution, tail is where new commands are added.
	 */
	PGcmdQueueEntry *cmd_queue_head;
	PGcmdQueueEntry *cmd_queue_tail;

	/*
	 * To save malloc traffic, we don't free entries right away; instead we
	 * save them in this list for possible reuse.
	 */
	PGcmdQueueEntry *cmd_queue_recycle;

	/* Connection data */
	pgsocket	sock;			/* FD for socket, PGINVALID_SOCKET if
								 * unconnected */
	SockAddr	laddr;			/* Local address */
	SockAddr	raddr;			/* Remote address */
	ProtocolVersion pversion;	/* FE/BE protocol version in use */
	int			sversion;		/* server version, e.g. 70401 for 7.4.1 */
	bool		auth_req_received;	/* true if any type of auth req received */
	bool		password_needed;	/* true if server demanded a password */
	bool		gssapi_used;	/* true if authenticated via gssapi */
	bool		sigpipe_so;		/* have we masked SIGPIPE via SO_NOSIGPIPE? */
	bool		sigpipe_flag;	/* can we mask SIGPIPE via MSG_NOSIGNAL? */
	bool		write_failed;	/* have we had a write failure on sock? */
	char	   *write_err_msg;	/* write error message, or NULL if OOM */

	bool		auth_required;	/* require an authentication challenge from
								 * the server? */
	uint32		allowed_auth_methods;	/* bitmask of acceptable AuthRequest
										 * codes */
	bool		client_finished_auth;	/* have we finished our half of the
										 * authentication exchange? */


	/* Transient state needed while establishing connection */
	PGTargetServerType target_server_type;	/* desired session properties */
	PGLoadBalanceType load_balance_type;	/* desired load balancing
											 * algorithm */
	bool		try_next_addr;	/* time to advance to next address/host? */
	bool		try_next_host;	/* time to advance to next connhost[]? */
	int			naddr;			/* number of addresses returned by getaddrinfo */
	int			whichaddr;		/* the address currently being tried */
	AddrInfo   *addr;			/* the array of addresses for the currently
								 * tried host */
	bool		send_appname;	/* okay to send application_name? */

	/* Miscellaneous stuff */
	int			be_pid;			/* PID of backend --- needed for cancels */
	int			be_key;			/* key of backend --- needed for cancels */
	pgParameterStatus *pstatus; /* ParameterStatus data */
	int			client_encoding;	/* encoding id */
	bool		std_strings;	/* standard_conforming_strings */
	PGTernaryBool default_transaction_read_only;	/* default_transaction_read_only */
	PGTernaryBool in_hot_standby;	/* in_hot_standby */
	PGVerbosity verbosity;		/* error/notice message verbosity */
	PGContextVisibility show_context;	/* whether to show CONTEXT field */
	PGlobjfuncs *lobjfuncs;		/* private state for large-object access fns */
	pg_prng_state prng_state;	/* prng state for load balancing connections */


	/* Buffer for data received from backend and not yet processed */
	char	   *inBuffer;		/* currently allocated buffer */
	int			inBufSize;		/* allocated size of buffer */
	int			inStart;		/* offset to first unconsumed data in buffer */
	int			inCursor;		/* next byte to tentatively consume */
	int			inEnd;			/* offset to first position after avail data */

	/* Buffer for data not yet sent to backend */
	char	   *outBuffer;		/* currently allocated buffer */
	int			outBufSize;		/* allocated size of buffer */
	int			outCount;		/* number of chars waiting in buffer */

	/* State for constructing messages in outBuffer */
	int			outMsgStart;	/* offset to msg start (length word); if -1,
								 * msg has no length word */
	int			outMsgEnd;		/* offset to msg end (so far) */

	/* Row processor interface workspace */
	PGdataValue *rowBuf;		/* array for passing values to rowProcessor */
	int			rowBufLen;		/* number of entries allocated in rowBuf */

	/*
	 * Status for asynchronous result construction.  If result isn't NULL, it
	 * is a result being constructed or ready to return.  If result is NULL
	 * and error_result is true, then we need to return a PGRES_FATAL_ERROR
	 * result, but haven't yet constructed it; text for the error has been
	 * appended to conn->errorMessage.  (Delaying construction simplifies
	 * dealing with out-of-memory cases.)  If saved_result isn't NULL, it is a
	 * PGresult that will replace "result" after we return that one; we use
	 * that in partial-result mode to remember the query's tuple metadata.
	 */
	PGresult   *result;			/* result being constructed */
	bool		error_result;	/* do we need to make an ERROR result? */
	PGresult   *saved_result;	/* original, empty result in partialResMode */

	/* Assorted state for SASL, SSL, GSS, etc */
	const pg_fe_sasl_mech *sasl;
	void	   *sasl_state;
	int			scram_sha_256_iterations;

	uint8		allowed_enc_methods;
	uint8		failed_enc_methods;
	uint8		current_enc_method;

	/* SSL structures */
	bool		ssl_in_use;
	bool		ssl_handshake_started;
	bool		ssl_cert_requested; /* Did the server ask us for a cert? */
	bool		ssl_cert_sent;	/* Did we send one in reply? */

#ifdef USE_SSL
#ifdef USE_OPENSSL
	SSL		   *ssl;			/* SSL status, if have SSL connection */
	X509	   *peer;			/* X509 cert of server */
#ifdef USE_SSL_ENGINE
	ENGINE	   *engine;			/* SSL engine, if any */
#else
	void	   *engine;			/* dummy field to keep struct the same if
								 * OpenSSL version changes */
#endif
	bool		crypto_loaded;	/* Track if libcrypto locking callbacks have
								 * been done for this connection. This can be
								 * removed once support for OpenSSL 1.0.2 is
								 * removed as this locking is handled
								 * internally in OpenSSL >= 1.1.0. */
#endif							/* USE_OPENSSL */
#endif							/* USE_SSL */

#ifdef ENABLE_GSS
	gss_ctx_id_t gctx;			/* GSS context */
	gss_name_t	gtarg_nam;		/* GSS target name */

	/* The following are encryption-only */
	bool		gssenc;			/* GSS encryption is usable */
	gss_cred_id_t gcred;		/* GSS credential temp storage. */

	/* GSS encryption I/O state --- see fe-secure-gssapi.c */
	char	   *gss_SendBuffer; /* Encrypted data waiting to be sent */
	int			gss_SendLength; /* End of data available in gss_SendBuffer */
	int			gss_SendNext;	/* Next index to send a byte from
								 * gss_SendBuffer */
	int			gss_SendConsumed;	/* Number of source bytes encrypted but
									 * not yet reported as sent */
	char	   *gss_RecvBuffer; /* Received, encrypted data */
	int			gss_RecvLength; /* End of data available in gss_RecvBuffer */
	char	   *gss_ResultBuffer;	/* Decryption of data in gss_RecvBuffer */
	int			gss_ResultLength;	/* End of data available in
									 * gss_ResultBuffer */
	int			gss_ResultNext; /* Next index to read a byte from
								 * gss_ResultBuffer */
	uint32		gss_MaxPktSize; /* Maximum size we can encrypt and fit the
								 * results into our output buffer */
#endif

#ifdef ENABLE_SSPI
	CredHandle *sspicred;		/* SSPI credentials handle */
	CtxtHandle *sspictx;		/* SSPI context */
	char	   *sspitarget;		/* SSPI target name */
	int			usesspi;		/* Indicate if SSPI is in use on the
								 * connection */
#endif

	/*
	 * Buffer for current error message.  This is cleared at the start of any
	 * connection attempt or query cycle; after that, all code should append
	 * messages to it, never overwrite.
	 *
	 * In some situations we might report an error more than once in a query
	 * cycle.  If so, errorMessage accumulates text from all the errors, and
	 * errorReported tracks how much we've already reported, so that the
	 * individual error PGresult objects don't contain duplicative text.
	 */
	PQExpBufferData errorMessage;	/* expansible string */
	int			errorReported;	/* # bytes of string already reported */

	/* Buffer for receiving various parts of messages */
	PQExpBufferData workBuffer; /* expansible string */
};
```
## Detailed Description
The  structure is the central data structure in libpq that represents a single database connection. It maintains all state information necessary for communicating with a PostgreSQL server, including connection parameters, authentication credentials, protocol state, I/O buffers, and error handling.

This structure supports advanced PostgreSQL features including:
- Multi-host connections for high availability and load balancing
- SSL/TLS encryption with comprehensive certificate management
- GSSAPI and SSPI authentication methods
- Pipeline mode for improved performance
- Asynchronous connection and query operations
- COPY operations for bulk data transfer
- Large object operations
- Connection pooling and cancellation support

The structure is designed to handle the complete lifecycle of a database connection from initial parameter parsing through connection establishment, query execution, and eventual cleanup.

## Parameters / Member Variables

### Connection Configuration
- : Host name, IP address, Unix socket path, or comma-separated list for multi-host connections
- : Numeric IP address(es) that take precedence over pghost
- : Port number(s) for database connections
- : Connection establishment timeout in seconds
- : TCP user timeout for network operations

### Authentication and Security
- : Target database name
- : PostgreSQL username
- : Password (if provided directly)
- : Path to file containing passwords
- : SSL connection mode (require, prefer, allow, disable)
- /: Client SSL certificate and key files
- : Whether authentication challenge is required
- : Bitmask of acceptable authentication methods

### Connection State Management
- : Overall connection status (CONNECTION_OK, CONNECTION_BAD, etc.)
- : Status of asynchronous operations
- : Current transaction status
- : Pipeline mode status
- : Whether using non-blocking I/O semantics

### Multi-host Support
- : Number of hosts specified in connection string
- : Index of currently active host
- : Array of pg_conn_host structures with host details
- : IP address of current network connection

### I/O Buffer Management
- /: Input buffer for data from server
- //: Input buffer position tracking
- /: Output buffer for data to server
- : Number of bytes waiting in output buffer

### Protocol Information
- : Socket file descriptor for network connection
- : Frontend/backend protocol version
- : Server version number
- /: Backend process ID and key for cancellation

### Error and Message Handling
- : Expandable buffer for current error messages
- : Temporary buffer for message construction
- : Callback procedures for notice processing

## Dependencies
- Functions called/Symbols referenced:
  - pg_conn_host (for multi-host connection support)
  - PGNoticeHooks (notice message processing)
  - ConnStatusType, PGAsyncStatusType (status enumerations)
  - PQExpBufferData (expandable string buffers)
  - Various SSL, GSS, and SSPI types for security features

- Called from (representative examples):
  - PQconnectdb, PQconnectdbParams (connection establishment functions)
  - PQexec, PQexecParams (query execution functions)
  - All libpq public API functions that operate on connections

## Notes and Other Information
- Located in libpq-int.h:365-650, this is the largest and most central structure in libpq
- The structure is opaque to client applications; all access is through libpq API functions
- Supports both blocking and non-blocking I/O modes for different application architectures
- Multi-host functionality enables automatic failover and load balancing across multiple database servers
- Extensive SSL/TLS support includes certificate verification, SNI, and multiple protocol versions
- Pipeline mode allows multiple queries to be sent before reading results, improving performance
- The structure is designed to be thread-safe when used properly (one connection per thread)
- Memory management is handled internally by libpq, with automatic buffer expansion as needed
- Error messages accumulate in errorMessage buffer and are reported through PGresult objects
- The structure supports PostgreSQL-specific features like LISTEN/NOTIFY, large objects, and COPY operations