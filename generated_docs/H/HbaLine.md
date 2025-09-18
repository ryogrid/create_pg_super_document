# HbaLine

## Location
src/include/libpq/hba.h: 94 - 138

## Overview
HbaLine is a comprehensive structure that represents a single parsed line from the pg_hba.conf configuration file, containing all authentication and authorization parameters for host-based access control.

## Definition


## Detailed Description
HbaLine represents the complete parsed and structured form of a single line from PostgreSQL's host-based authentication configuration file (pg_hba.conf). This structure encapsulates all possible authentication parameters and connection restrictions that can be specified in an HBA entry, including connection type, database/role matching criteria, network address restrictions, and authentication method configuration. The structure supports multiple authentication methods including PAM, LDAP, Kerberos, RADIUS, and certificate-based authentication, with dedicated fields for each method's specific parameters.

## Parameters / Member Variables
- : Path to the configuration file containing this line
- : Line number within the source file for error reporting
- : Original unparsed line text from the configuration file
- : Type of connection (local, host, hostssl, hostnossl, hostgssenc, hostnogssenc)
- : List of database names that this rule applies to
- : List of user/role names that this rule applies to
- : Network address for host-based connections (sockaddr_storage structure)
- : Length of the address structure (zero if no valid address)
- : Network mask for address range matching
- : Length of the mask structure (zero if no valid mask)
- : Method for IP address comparison (exact, CIDR, hostname lookup)
- DESKTOP-IOASPN6: Hostname for hostname-based address matching
- : Authentication method to use (trust, reject, md5, password, gss, sspi, ident, peer, ldap, radius, cert, pam, bsd)
- : Name of user mapping configuration for ident/peer authentication
- : PAM service name for PAM authentication
- : Whether to include hostname in PAM authentication
- : Whether to use TLS for LDAP connections
- : LDAP URL scheme (ldap or ldaps)
- : LDAP server hostname or IP address
- : LDAP server port number
- : Distinguished name for LDAP bind operations
- : Password for LDAP bind operations
- : LDAP attribute to search for username
- : LDAP search filter template
- : Base distinguished name for LDAP searches
- : LDAP search scope (base, one, sub)
- : Prefix to add to username for LDAP simple bind
- : Suffix to add to username for LDAP simple bind
- : Client certificate verification mode
- : How to extract username from client certificate
- : Kerberos realm for GSS authentication
- : Whether to include realm in Kerberos principal matching
- : Whether to use PostgreSQL 8.1 realm handling compatibility
- : Whether to use UPN format for Kerberos usernames
- : List of RADIUS server addresses
- : String representation of RADIUS servers
- : List of RADIUS shared secrets
- : String representation of RADIUS secrets
- : List of RADIUS NAS identifiers
- : String representation of RADIUS identifiers
- : List of RADIUS server port numbers
- : String representation of RADIUS ports

## Dependencies
- Functions called/Symbols referenced:
  - [ConnType](../C/ConnType.md)
  - [IPCompareMethod](../I/IPCompareMethod.md)
  - UserAuth
  - [ClientCertMode](../C/ClientCertMode.md)
  - ClientCertName
  - [List](../L/List.md) (PostgreSQL list structure)
  - sockaddr_storage (POSIX socket address structure)
- Called from (representative examples):
  - token_matches_insensitive
  - [parse_hba_line](../p/parse_hba_line.md)
  - [parse_hba_auth_opt](../p/parse_hba_auth_opt.md)
  - [check_hba](../c/check_hba.md)
  - [load_hba](../l/load_hba.md)
  - [get_hba_options](../g/get_hba_options.md)
  - [fill_hba_line](../f/fill_hba_line.md)
  - [fill_hba_view](../f/fill_hba_view.md)

## Notes and Other Information
- This structure is the core data structure for PostgreSQL's host-based authentication system
- Contains fields for all supported authentication methods, though only relevant fields are populated based on the specific auth_method
- The structure includes both parsed binary data (like sockaddr_storage) and original string representations for some fields
- Memory management and parsing logic is handled by functions in src/backend/libpq/hba.c
- Used extensively in authentication decision-making during client connection establishment
- The various _s suffixed fields store string representations that correspond to parsed list fields for certain authentication parameters