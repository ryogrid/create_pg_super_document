# CheckCertAuth

## Location
[src/backend/libpq/auth.c:2689-2778](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth.c#L2689-L2778)

## Overview
Performs SSL client certificate authentication by validating the certificate's subject DN or CN against the requested username using usermap rules.

## Definition

```c
typedef struct
{
	uint8		attribute;
	uint8		length;
	uint8		data[FLEXIBLE_ARRAY_MEMBER];
} radius_attribute;
```
## Detailed Description
The `CheckCertAuth` function implements PostgreSQL's SSL client certificate authentication mechanism. It validates that a client's SSL certificate contains appropriate identity information and that this identity maps correctly to the requested PostgreSQL username.

The function operates in different modes based on the HBA configuration:

1. **Certificate DN authentication**: Uses the complete Distinguished Name (DN) from the certificate's subject
2. **Certificate CN authentication**: Uses only the Common Name (CN) component from the certificate's subject

The function first extracts the appropriate identity information from the certificate (either DN or CN based on `clientcertname` configuration), then validates this information against the requested username using PostgreSQL's usermap mechanism. For pure certificate authentication (`uaCert` method), the function sets the authenticated identity to the certificate's subject DN.

The function also handles error reporting for different certificate validation scenarios, providing specific error messages when `clientcert=verify-full` is configured.

## Parameters
- `port`: Pointer to the Port structure containing SSL connection information and HBA configuration, including certificate data (`peer_dn`, `peer_cn`) and authentication settings

## Dependencies
- Functions called/Symbols referenced:
  - [check_usermap](../c/check_usermap.md): Validate username mapping against certificate identity
  - [set_authn_id](../s/set_authn_id.md): Set the authenticated identity for logging and auditing
  - `ereport`: Generate error messages and log entries
  - `strlen`: Check certificate identity string length
  - clientCertDN, clientCertCN: Constants defining certificate name extraction modes
  - clientCertFull: Constant for full certificate verification mode
  - uaCert: Constant for certificate authentication method
  - STATUS_OK, STATUS_ERROR: Authentication result constants
- Called from (representative examples):
  - Authentication dispatch logic in auth.c at line 648 (when SSL certificate validation is required)

## Dependencies
- Functions called/Symbols referenced:
  - [check_usermap](../c/check_usermap.md): Validates username mapping
  - [set_authn_id](../s/set_authn_id.md): Sets authenticated identity
  - `ereport`: Error reporting
  - Various certificate and authentication constants
- Called from (representative examples):
  - Main authentication flow at src/backend/libpq/auth.c:648

## Notes and Other Information
- Requires SSL to be compiled in (protected by `#ifdef USE_SSL` in caller)
- Asserts that `port->ssl` is valid, indicating an active SSL connection
- Returns the result of `check_usermap` which can be `STATUS_OK` or `STATUS_ERROR`
- For `uaCert` method, sets the authenticated identity to the certificate's subject DN regardless of whether DN or CN is used for authorization
- Provides detailed error messages for different failure scenarios, particularly when `clientcert=verify-full` is configured
- Supports both DN and CN based certificate authentication depending on HBA configuration
- Integrates with PostgreSQL's usermap system to allow flexible username mapping rules
- Used in conjunction with other authentication methods when certificate validation is required
- Certificate information (`peer_dn`, `peer_cn`) must be populated by SSL handshake process before this function is called

## Simplified Source

```c
static int
CheckCertAuth(Port *port)
{
    int         status_check_usermap = STATUS_ERROR;
    char       *peer_username = NULL;

    Assert(port->ssl);

    // Select certificate identity field to use (DN or CN)
    switch (port->hba->clientcertname)
    {
        case clientCertDN:
            peer_username = port->peer_dn;
            break;
        case clientCertCN:
            peer_username = port->peer_cn;
    }

    // Validate that certificate contains user identity
    if (peer_username == NULL || strlen(peer_username) <= 0)
    {
        ereport(LOG, (errmsg("certificate authentication failed for user \"%s\": "
                             "client certificate contains no user name", port->user_name)));
        return STATUS_ERROR;
    }

    if (port->hba->auth_method == uaCert)
    {
        // For pure cert auth, set authenticated identity to subject DN
        if (!port->peer_dn)
        {
            ereport(LOG, (errmsg("certificate authentication failed for user \"%s\": "
                                 "unable to retrieve subject DN", port->user_name)));
            return STATUS_ERROR;
        }

        set_authn_id(port, port->peer_dn);
    }

    // Check if certificate identity maps to requested username
    status_check_usermap = check_usermap(port->hba->usermap, port->user_name, peer_username, false);

    if (status_check_usermap != STATUS_OK)
    {
        // Log specific error for verify-full mode
        if (port->hba->clientcert == clientCertFull && port->hba->auth_method != uaCert)
        {
            switch (port->hba->clientcertname)
            {
                case clientCertDN:
                    ereport(LOG, (errmsg("certificate validation (clientcert=verify-full) "
                                         "failed for user \"%s\": DN mismatch", port->user_name)));
                    break;
                case clientCertCN:
                    ereport(LOG, (errmsg("certificate validation (clientcert=verify-full) "
                                         "failed for user \"%s\": CN mismatch", port->user_name)));
            }
        }
    }

    return status_check_usermap;
}
```