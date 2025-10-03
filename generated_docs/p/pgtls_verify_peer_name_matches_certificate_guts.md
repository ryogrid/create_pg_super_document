# pgtls_verify_peer_name_matches_certificate_guts

## Location
[src/interfaces/libpq/fe-secure-openssl.c:574-724](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-secure-openssl.c#L574-L724)

## Overview
Core function that verifies whether a server certificate matches the hostname that a client connected to, implementing certificate name validation according to RFC 2818 and RFC 6125 with some practical deviations.

## Definition

```c
int
pgtls_verify_peer_name_matches_certificate_guts(PGconn *conn,
												int *names_examined,
												char **first_name)
```
## Detailed Description
This function performs SSL/TLS certificate hostname verification by comparing the hostname used to connect to the server against names present in the server's certificate. The verification process follows a specific priority order:

1. **Subject Alternative Names (SANs)**: First checks certificate SANs for matching entries
   - For DNS hostnames: looks for dNSName entries
   - For IP addresses: looks for iPAddress entries
   
2. **Common Name (CN) fallback**: If no matching SAN of the appropriate type is found, falls back to checking the certificate's Common Name field

The implementation deviates from strict RFC compliance in one key area: when connecting to an IP address, it allows CN matching even if dNSName SANs are present, which RFC 6125 prohibits but provides more intuitive behavior.

## Parameters / Member Variables
- `*conn`: PostgreSQL connection object containing the peer certificate and connection details
- `*names_examined`: Output parameter that returns the total number of certificate names examined during verification
- `**first_name`: Output parameter that returns the first name found in the certificate (for error reporting)
## Dependencies
- Functions called/Symbols referenced:
  - : Determines if hostname is an IP address vs DNS name
  - : Performs DNS name matching
  - : Performs IP address matching
- Called from (representative examples):
  - : Main certificate verification entry point
  - : Thread management context

## Notes and Other Information
- Return value: 0 for no match, positive for successful match, negative for error
- Implements NSS-like behavior rather than strict RFC compliance for better usability
- Handles both IPv4 and IPv6 address validation
- Manages memory allocation for extracted certificate names
- Uses OpenSSL APIs for certificate parsing and SAN extraction
- Located in 
- Prior libpq versions did not consider iPAddress SANs, so this implementation may break certificates with different IPs in CN vs SANs