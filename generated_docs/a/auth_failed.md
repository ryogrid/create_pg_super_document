# auth_failed

## Location
[src/backend/libpq/auth.c:254-351](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L254-L351)

## Overview
Handles authentication failure by sending an appropriate error message to the client and terminating the connection, providing different error messages based on the authentication method while balancing security concerns with user experience.

## Definition

```c
enumber,
					   port->hba->rawline);
```
## Detailed Description
The  function is responsible for handling authentication failures in PostgreSQL's connection process. It carefully constructs error messages that inform the user about the authentication failure without revealing sensitive security information. The function takes into account the specific authentication method that failed and provides method-specific error messages.

The function implements a security-conscious approach by not revealing detailed failure reasons to potential attackers while still providing useful information to legitimate users. It logs additional details to the postmaster log for administrators to investigate issues.

For EOF status (client disconnection), the function simply exits without sending messages, as there's no client to respond to and logging such events would create noise in logs (especially common with password authentication).

## Parameters / Member Variables
- : Pointer to the Port structure containing connection and authentication information
- : Integer status code indicating the type of failure (e.g., STATUS_EOF)
- : Optional string containing additional details to be logged (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [proc_exit](../p/proc_exit.md) (for STATUS_EOF handling)
  - [psprintf](../p/psprintf.md) (for formatting error messages)
  - ereport (for sending the error to client and logs)
  - gettext_noop (for internationalization)
  - [errdetail_log](../e/errdetail_log.md) (for detailed logging)
- Called from (representative examples):
  - HOSTNAME_LOOKUP_DETAIL (based on references found)
- Authentication method constants used:
  - uaReject, uaImplicitReject, uaTrust, uaIdent, uaPeer
  - uaPassword, uaMD5, uaSCRAM, uaGSS, uaSSPI
  - uaPAM, uaBSD, uaLDAP, uaCert, uaRADIUS

## Notes and Other Information
- The function never returns - it always terminates the process with ereport(FATAL)
- Different error codes are used: ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION (default) and ERRCODE_INVALID_PASSWORD (for password-based methods)
- The function includes HBA (Host-Based Authentication) line information in the detailed log to help administrators identify which pg_hba.conf rule was matched
- Special handling for STATUS_EOF prevents log spam from normal client disconnections during password challenges

## Simplified Source

```c
static void
auth_failed(Port *port, int status, const char *logdetail)
{
    const char *errstr;
    char *cdetail;
    int errcode_return = ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION;

    // Handle client disconnection - just exit without logging
    if (status == STATUS_EOF)
        proc_exit(0);

    // Select appropriate error message based on auth method
    switch (port->hba->auth_method) {
        case uaReject:
        case uaImplicitReject:
            errstr = gettext_noop("authentication failed for user \"%s\": host rejected");
            break;
        case uaPassword:
        case uaMD5:
        case uaSCRAM:
            errstr = gettext_noop("password authentication failed for user \"%s\"");
            errcode_return = ERRCODE_INVALID_PASSWORD;
            break;
        case uaGSS:
            errstr = gettext_noop("GSSAPI authentication failed for user \"%s\"");
            break;
        case uaSSPI:
            errstr = gettext_noop("SSPI authentication failed for user \"%s\"");
            break;
        // Additional auth methods: uaTrust, uaIdent, uaPeer, uaPAM,
        // uaBSD, uaLDAP, uaCert, uaRADIUS...
        default:
            errstr = gettext_noop("authentication failed for user \"%s\": invalid authentication method");
            break;
    }

    // Build connection details for logging
    cdetail = psprintf("Connection matched file \"%s\" line %d: \"%s\"",
                      port->hba->sourcefile, port->hba->linenumber,
                      port->hba->rawline);

    if (logdetail)
        logdetail = psprintf("%s\n%s", logdetail, cdetail);
    else
        logdetail = cdetail;

    // Send fatal error to client and terminate
    ereport(FATAL,
            (errcode(errcode_return),
             errmsg(errstr, port->user_name),
             logdetail ? errdetail_log("%s", logdetail) : 0));
}
```