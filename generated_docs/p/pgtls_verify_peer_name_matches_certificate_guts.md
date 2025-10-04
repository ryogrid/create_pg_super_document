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

## Simplified Source
```c
int pgtls_verify_peer_name_matches_certificate_guts(PGconn *conn,
                                                   int *names_examined,
                                                   char **first_name) {
    STACK_OF(GENERAL_NAME) *peer_san;
    int rc = 0;
    char *host = conn->connhost[conn->whichhost].host;
    int host_type;
    bool check_cn = true;

    // Determine if host is IP address or DNS name
    if (is_ip_address(host))
        host_type = GEN_IPADD;
    else
        host_type = GEN_DNS;

    // Get Subject Alternative Names from certificate
    peer_san = X509_get_ext_d2i(conn->peer, NID_subject_alt_name, NULL, NULL);

    if (peer_san) {
        int san_len = sk_GENERAL_NAME_num(peer_san);

        // Check each SAN entry
        for (int i = 0; i < san_len; i++) {
            const GENERAL_NAME *name = sk_GENERAL_NAME_value(peer_san, i);
            char *alt_name = NULL;

            // If SAN type matches host type, don't fallback to CN
            if (name->type == host_type)
                check_cn = false;

            // Verify DNS names
            if (name->type == GEN_DNS) {
                (*names_examined)++;
                rc = openssl_verify_peer_name_matches_certificate_name(conn,
                                                                      name->d.dNSName,
                                                                      &alt_name);
            }
            // Verify IP addresses
            else if (name->type == GEN_IPADD) {
                (*names_examined)++;
                rc = openssl_verify_peer_name_matches_certificate_ip(conn,
                                                                    name->d.iPAddress,
                                                                    &alt_name);
            }

            // Store first name found for error reporting
            if (alt_name) {
                if (!*first_name)
                    *first_name = alt_name;
                else
                    free(alt_name);
            }

            // Stop on match or error
            if (rc != 0) {
                check_cn = false;
                break;
            }
        }
        sk_GENERAL_NAME_pop_free(peer_san, GENERAL_NAME_free);
    }

    // Fallback to Common Name if no matching SAN found
    if (check_cn) {
        X509_NAME *subject_name = X509_get_subject_name(conn->peer);
        if (subject_name != NULL) {
            int cn_index = X509_NAME_get_index_by_NID(subject_name, NID_commonName, -1);
            if (cn_index >= 0) {
                char *common_name = NULL;
                (*names_examined)++;

                rc = openssl_verify_peer_name_matches_certificate_name(conn,
                    X509_NAME_ENTRY_get_data(X509_NAME_get_entry(subject_name, cn_index)),
                    &common_name);

                if (common_name) {
                    if (!*first_name)
                        *first_name = common_name;
                    else
                        free(common_name);
                }
            }
        }
    }

    return rc;
}
```