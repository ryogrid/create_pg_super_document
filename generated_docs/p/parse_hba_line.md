# parse_hba_line

## Location
[src/backend/libpq/hba.c:1322-2048](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L1322-L2048)

## Overview
Parses a tokenized line from the PostgreSQL host-based authentication (HBA) configuration file and converts it into a structured HbaLine representation.

## Definition

```c
struct addrinfo *gai_result;
```
## Detailed Description
The  function is the core parser for PostgreSQL's host-based authentication system, responsible for converting tokenized pg_hba.conf entries into structured  objects. This is a complex function that handles all aspects of HBA line parsing including:

1. **Connection Type Parsing**: Supports 'local', 'host', 'hostssl', 'hostnossl', 'hostgssenc', and 'hostnogssenc' connection types with appropriate validation for SSL and GSSAPI support
2. **Database and Role Specification**: Parses database and role lists, supporting regular expressions through 
3. **Address/Network Parsing**: Handles various address formats including:
   - Special keywords: 'all', 'samehost', 'samenet'
   - IP addresses with CIDR notation (e.g., 192.168.1.0/24)
   - Hostnames
   - Separate IP and netmask specifications
4. **Authentication Method Parsing**: Supports all PostgreSQL authentication methods (trust, reject, md5, scram-sha-256, peer, ident, password, gss, sspi, pam, bsd, ldap, cert, radius) with compile-time feature validation
5. **Authentication Options**: Parses method-specific options in name=value format
6. **Configuration Validation**: Performs extensive validation including:
   - Mandatory parameter checking for specific auth methods
   - Invalid combination detection (e.g., peer auth on non-local connections)
   - SSL/GSSAPI availability validation
   - LDAP and RADIUS configuration consistency checks

The function uses comprehensive error reporting with context-specific messages and line number information. It allocates memory in the current memory context and expects the caller to handle cleanup on errors.

## Parameters / Member Variables
- : Pointer to TokenizedAuthLine structure containing parsed tokens, line number, filename, and error message storage
- : Error reporting level for ereport calls (typically LOG, WARNING, or ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - : Allocates zero-initialized memory for HbaLine structure
  - : Duplicates strings in PostgreSQL memory context
  - , , : PostgreSQL list manipulation functions
  - : Creates copies of authentication tokens
  - : Compiles regular expressions for database/role matching
  - : Checks if token matches specific keywords
  - : Resolves hostnames and IP addresses
  - : Generates netmasks from CIDR notation
  - : Parses authentication method options
  - : Converts getaddrinfo error codes to strings
  - : Frees address info structures
  - : PostgreSQL error reporting system
  - Various authentication method constants (uaTrust, uaGSS, etc.)
  - Connection type constants (ctLocal, ctHostSSL, etc.)
- Called from:
  - : Main HBA file loading function at src/backend/libpq/hba.c:2620
  - : System view population function at src/backend/utils/adt/hbafuncs.c:405

## Notes and Other Information
- This function is one of the largest and most complex in the PostgreSQL HBA system, handling all parsing logic for pg_hba.conf entries
- Memory management uses PostgreSQL's context system - the function may leak memory on error, expecting the caller to reset the memory context
- The function performs extensive validation at parse time to catch configuration errors early
- Feature availability checking ensures that authentication methods requiring specific compile-time options (SSL, GSSAPI, PAM, etc.) are properly validated
- IPv4 and IPv6 addresses are supported throughout, with family-specific validation
- The function handles backward compatibility (e.g., converting 'ident' to 'peer' for local connections)
- RADIUS and LDAP authentication methods have complex parameter validation logic for server lists and configuration consistency
- Error messages include line numbers and file context for administrator debugging
- Default values are set for certain authentication methods (e.g., include_realm for GSS/SSPI)
- The function supports both CIDR notation and separate IP/netmask specifications for network ranges
- Regular expression compilation is performed at parse time for database and role pattern matching

## Simplified Source

```c
// Simplified version of parse_hba_line
HbaLine *parse_hba_line(TokenizedAuthLine *tok_line, int elevel) {
    HbaLine *parsedline;
    AuthToken *token;
    List *tokens;
    ListCell *field;
    char *str, *cidr_slash;

    // Initialize parsed line structure
    parsedline = palloc0(sizeof(HbaLine));
    parsedline->sourcefile = pstrdup(tok_line->file_name);
    parsedline->linenumber = tok_line->line_num;
    parsedline->rawline = pstrdup(tok_line->raw_line);

    // Parse connection type (local, host, hostssl, etc.)
    field = list_head(tok_line->fields);
    tokens = lfirst(field);
    if (tokens->length > 1) {
        report_error("multiple values specified for connection type");
        return NULL;
    }

    token = linitial(tokens);
    if (strcmp(token->string, "local") == 0) {
        parsedline->conntype = ctLocal;
    } else if (starts_with_host(token->string)) {
        // Parse host variants (host, hostssl, hostnossl, hostgssenc, hostnogssenc)
        parsedline->conntype = determine_host_connection_type(token->string);
        validate_ssl_gss_support(parsedline->conntype, elevel);
    } else {
        report_error("invalid connection type");
        return NULL;
    }

    // Parse database list
    field = lnext(tok_line->fields, field);
    if (!field) {
        report_error("missing database specification");
        return NULL;
    }
    parsedline->databases = parse_token_list(lfirst(field));

    // Parse role list
    field = lnext(tok_line->fields, field);
    if (!field) {
        report_error("missing role specification");
        return NULL;
    }
    parsedline->roles = parse_token_list(lfirst(field));

    // Parse IP address/hostname (for non-local connections)
    if (parsedline->conntype != ctLocal) {
        field = lnext(tok_line->fields, field);
        if (!field) {
            report_error("missing IP address specification");
            return NULL;
        }

        tokens = lfirst(field);
        token = linitial(tokens);

        if (token_is_keyword(token, "all")) {
            parsedline->ip_cmp_method = ipCmpAll;
        } else if (token_is_keyword(token, "samehost")) {
            parsedline->ip_cmp_method = ipCmpSameHost;
        } else if (token_is_keyword(token, "samenet")) {
            parsedline->ip_cmp_method = ipCmpSameNet;
        } else {
            // Parse IP address with optional CIDR or separate netmask
            parse_ip_address_and_mask(token, field, parsedline);
        }
    }

    // Parse authentication method
    field = lnext(tok_line->fields, field);
    if (!field) {
        report_error("missing authentication method");
        return NULL;
    }

    tokens = lfirst(field);
    token = linitial(tokens);
    parsedline->auth_method = parse_auth_method(token->string);

    // Validate authentication method compatibility
    validate_auth_method_compatibility(parsedline);

    // Parse authentication options (name=value pairs)
    while ((field = lnext(tok_line->fields, field)) != NULL) {
        parse_auth_options(lfirst(field), parsedline);
    }

    // Validate mandatory arguments for specific auth methods
    validate_mandatory_auth_args(parsedline);

    // Set default values for certain auth methods
    set_auth_method_defaults(parsedline);

    return parsedline;
}
```

Key simplifications made:
- Consolidated error handling into helper functions like `report_error()`
- Abstracted complex IP address parsing into `parse_ip_address_and_mask()`
- Simplified connection type parsing with helper function `determine_host_connection_type()`
- Created helper functions for validation: `validate_ssl_gss_support()`, `validate_auth_method_compatibility()`, `validate_mandatory_auth_args()`
- Abstracted authentication method parsing into `parse_auth_method()`
- Removed detailed platform-specific conditional compilation blocks
- Consolidated token list processing into `parse_token_list()`
- Focused on the main execution flow while abstracting complex validation logic
- Simplified the authentication options parsing with `parse_auth_options()`
- Added `set_auth_method_defaults()` to handle default value assignment