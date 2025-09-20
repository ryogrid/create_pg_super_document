# pg_be_sasl_mech

## Location
[src/include/libpq/sasl.h:37-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/libpq/sasl.h#L37-L130)

## Overview
The  structure defines callback functions for implementing SASL (Simple Authentication and Security Layer) mechanisms in PostgreSQL's backend authentication system.

## Definition

```c
typedef struct pg_be_sasl_mech
{
	/*---------
	 * get_mechanisms()
	 *
	 * Retrieves the list of SASL mechanism names supported by this
	 * implementation.
	 *
	 * Input parameters:
	 *
	 *	port: The client Port
	 *
	 * Output parameters:
	 *
	 *	buf:  A StringInfo buffer that the callback should populate with
	 *		  supported mechanism names.  The names are appended into this
	 *		  StringInfo, each one ending with '\0' bytes.
	 *---------
	 */
	void		(*get_mechanisms) (Port *port, StringInfo buf);

	/*---------
	 * init()
	 *
	 * Initializes mechanism-specific state for a connection. This callback
	 * must return a pointer to its allocated state, which will be passed
	 * as-is as the first argument to the other callbacks.
	 *
	 * Input parameters:
	 *
	 *	port:        The client Port.
	 *
	 *	mech:        The actual mechanism name in use by the client.
	 *
	 *	shadow_pass: The stored secret for the role being authenticated, or
	 *				 NULL if one does not exist.  Mechanisms that do not use
	 *				 shadow entries may ignore this parameter.  If a
	 *				 mechanism uses shadow entries but shadow_pass is NULL,
	 *				 the implementation must continue the exchange as if the
	 *				 user existed and the password did not match, to avoid
	 *				 disclosing valid user names.
	 *---------
	 */
	void	   *(*init) (Port *port, const char *mech, const char *shadow_pass);

	/*---------
	 * exchange()
	 *
	 * Produces a server challenge to be sent to the client.  The callback
	 * must return one of the PG_SASL_EXCHANGE_* values, depending on
	 * whether the exchange continues, has finished successfully, or has
	 * failed.
	 *
	 * Input parameters:
	 *
	 *	state:	  The opaque mechanism state returned by init()
	 *
	 *	input:	  The response data sent by the client, or NULL if the
	 *			  mechanism is client-first but the client did not send an
	 *			  initial response.  (This can only happen during the first
	 *			  message from the client.)  This is guaranteed to be
	 *			  null-terminated for safety, but SASL allows embedded
	 *			  nulls in responses, so mechanisms must be careful to
	 *            check inputlen.
	 *
	 *	inputlen: The length of the challenge data sent by the server, or
	 *			  -1 if the client did not send an initial response
	 *
	 * Output parameters, to be set by the callback function:
	 *
	 *	output:    A palloc'd buffer containing either the server's next
	 *			   challenge (if PG_SASL_EXCHANGE_CONTINUE is returned) or
	 *			   the server's outcome data (if PG_SASL_EXCHANGE_SUCCESS is
	 *			   returned and the mechanism requires data to be sent during
	 *			   a successful outcome).  The callback should set this to
	 *			   NULL if the exchange is over and no output should be sent,
	 *			   which should correspond to either PG_SASL_EXCHANGE_FAILURE
	 *			   or a PG_SASL_EXCHANGE_SUCCESS with no outcome data.
	 *
	 *  outputlen: The length of the challenge data.  Ignored if *output is
	 *			   NULL.
	 *
	 *	logdetail: Set to an optional DETAIL message to be printed to the
	 *			   server log, to disambiguate failure modes.  (The client
	 *			   will only ever see the same generic authentication
	 *			   failure message.) Ignored if the exchange is completed
	 *			   with PG_SASL_EXCHANGE_SUCCESS.
	 *---------
	 */
	int			(*exchange) (void *state,
							 const char *input, int inputlen,
							 char **output, int *outputlen,
							 const char **logdetail);
} pg_be_sasl_mech;
```
## Detailed Description
The  structure serves as a callback interface for implementing backend SASL authentication mechanisms in PostgreSQL. It provides a standardized way to handle the three main phases of SASL authentication:

1. **Mechanism Discovery**: Listing supported SASL mechanisms
2. **Initialization**: Setting up mechanism-specific state for a connection
3. **Message Exchange**: Handling the challenge-response authentication flow

Each SASL mechanism implementation (such as SCRAM-SHA-256) provides concrete implementations of these callbacks. The structure is designed to be passed to  during client authentication once the server has decided which authentication method to use.

The interface supports both client-first and server-first SASL mechanisms and handles the complete authentication exchange until success, failure, or continuation.

## Parameters / Member Variables
- : Function pointer that retrieves the list of SASL mechanism names supported by this implementation
  - Input:  (client Port),  (StringInfo buffer to populate with mechanism names)
  - Each mechanism name is null-terminated in the buffer
- : Function pointer that initializes mechanism-specific state for a connection
  - Input:  (client Port),  (mechanism name in use),  (stored secret for the role, or NULL)
  - Returns: Opaque state pointer passed to other callbacks
  - Must handle NULL shadow_pass securely to avoid username disclosure
- : Function pointer that handles the SASL challenge-response exchange
  - Input:  (opaque mechanism state),  (client response data),  (response length)
  - Output:  (server challenge/outcome),  (challenge length),  (optional log message)
  - Returns: PG_SASL_EXCHANGE_CONTINUE (0), PG_SASL_EXCHANGE_SUCCESS (1), or PG_SASL_EXCHANGE_FAILURE (2)

## Dependencies
- Functions called/Symbols referenced:
  - [Port](../P/Port.md) (from libpq/libpq-be.h)
  - StringInfo (from lib/stringinfo.h)
  - PG_SASL_EXCHANGE_* constants (defined in same header)
- Called from (representative examples):
  - [CheckSASLAuth](../C/CheckSASLAuth.md) (src/backend/libpq/auth-sasl.c:52)
  - Referenced by SCRAM implementation (src/include/libpq/scram.h:25)

## Notes and Other Information
- The structure is defined in src/include/libpq/sasl.h:37-130
- SASL allows embedded nulls in responses, so implementations must check inputlen rather than relying on null termination
- Security consideration: When shadow_pass is NULL but the mechanism requires it, implementations should continue the exchange as if authentication failed to prevent username enumeration
- The exchange function should use palloc() for output buffers
- Output parameters are only meaningful when PG_SASL_EXCHANGE_CONTINUE or PG_SASL_EXCHANGE_SUCCESS is returned
- The logdetail parameter helps server administrators debug authentication issues without exposing sensitive information to clients