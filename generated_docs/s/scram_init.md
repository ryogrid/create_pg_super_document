# scram_init

## Location
[src/backend/libpq/auth-scram.c:236-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/auth-scram.c#L236-L347)

## Overview
Initializes a new SCRAM authentication exchange status tracker and prepares the authentication state for a client connection.

## Definition

```c
static void *
scram_init(Port *port, const char *selected_mech, const char *shadow_pass)
```
## Detailed Description
This function creates and initializes a scram_state structure to track the progress of a SCRAM authentication exchange. It validates the selected SASL mechanism, parses the stored password secret from pg_authid, and sets up the authentication context. If the user doesn't have a valid SCRAM secret or if a mock authentication is requested, the function still proceeds but marks the authentication as 'doomed' to fail later while maintaining timing-attack resistance.

The function supports two SCRAM mechanisms:
- SCRAM-SHA-256-PLUS (with channel binding, requires SSL)
- SCRAM-SHA-256 (standard variant)

When a valid secret cannot be obtained, the function creates a mock secret to prevent information leakage to attackers.

## Parameters / Member Variables
- `*port`: Connection port information containing user_name and SSL status
- `*selected_mech`: The SASL mechanism selected by the client (must be supported)
- `*shadow_pass`: The role's stored secret from pg_authid.rolpassword (can be NULL for mock auth)
## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md)
  - strcmp
  - [get_password_type](../g/get_password_type.md)
  - [parse_scram_secret](../p/parse_scram_secret.md)
  - [mock_scram_secret](../m/mock_scram_secret.md)
  - ereport/errmsg
  - [psprintf](../p/psprintf.md)
  - SCRAM_SHA_256_PLUS_NAME
  - SCRAM_SHA_256_NAME
  - PASSWORD_TYPE_SCRAM_SHA_256
  - SCRAM_AUTH_INIT
- Called from (representative examples):
  - Referenced as function pointer in pg_be_scram_mech structure
  - Used by SASL authentication framework during client authentication

## Notes and Other Information
- Returns a void pointer to the allocated scram_state structure
- This is a static function used as a callback in the SASL mechanism interface
- Performs timing-attack resistant mock authentication when secrets are unavailable
- Validates that channel binding variants are only used with SSL connections
- Sets the 'doomed' flag when authentication should fail (invalid secrets, mock auth)
- Logs detailed error information for debugging while avoiding information leakage
- The returned state must be freed by the caller after authentication completes

## Simplified Source

```c
static void *
scram_init(Port *port, const char *selected_mech, const char *shadow_pass)
{
    scram_state *state;
    bool got_secret;

    // Allocate and initialize SCRAM state
    state = (scram_state *) palloc0(sizeof(scram_state));
    state->port = port;
    state->state = SCRAM_AUTH_INIT;

    // Validate selected SASL mechanism
#ifdef USE_SSL
    if (strcmp(selected_mech, SCRAM_SHA_256_PLUS_NAME) == 0 && port->ssl_in_use)
        state->channel_binding_in_use = true;
    else
#endif
    if (strcmp(selected_mech, SCRAM_SHA_256_NAME) == 0)
        state->channel_binding_in_use = false;
    else
        ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
                errmsg("client selected an invalid SASL authentication mechanism")));

    // Parse the stored password secret
    if (shadow_pass)
    {
        int password_type = get_password_type(shadow_pass);

        if (password_type == PASSWORD_TYPE_SCRAM_SHA_256)
        {
            // Parse SCRAM secret components
            if (parse_scram_secret(shadow_pass, &state->iterations,
                                   &state->hash_type, &state->key_length,
                                   &state->salt,
                                   state->StoredKey, state->ServerKey))
                got_secret = true;
            else
            {
                ereport(LOG, (errmsg("invalid SCRAM secret for user \"%s\"",
                                     state->port->user_name)));
                got_secret = false;
            }
        }
        else
        {
            // User doesn't have SCRAM secret
            state->logdetail = psprintf(_("User \"%s\" does not have a valid SCRAM secret."),
                                        state->port->user_name);
            got_secret = false;
        }
    }
    else
        got_secret = false;

    // Use mock secret if real one unavailable (timing attack protection)
    if (!got_secret)
    {
        mock_scram_secret(state->port->user_name, &state->hash_type,
                          &state->iterations, &state->key_length,
                          &state->salt,
                          state->StoredKey, state->ServerKey);
        state->doomed = true;
    }

    return state;
}
```