# check_hostname

## Location
[src/backend/libpq/hba.c:1072-1162](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/hba.c#L1072-L1162)

## Overview
Verifies that a connecting client's IP address matches a given hostname by performing DNS resolution and reverse lookup validation.

## Definition

```c
struct addrinfo *gai_result,
			   *gai;
```
## Detailed Description
The  function is a critical component of PostgreSQL's host-based authentication (HBA) system that validates whether a client's IP address corresponds to a specified hostname in pg_hba.conf. It performs a two-stage verification process:

1. **Reverse DNS Lookup**: If not already cached, it resolves the client's IP address to a hostname using 
2. **Hostname Pattern Matching**: It checks if the resolved hostname matches the pattern specified in the HBA configuration using 
3. **Forward DNS Verification**: To prevent DNS spoofing attacks, it performs a forward lookup of the resolved hostname back to IP addresses and verifies that one of them matches the original client IP

The function implements caching of DNS resolution results in the  structure to avoid repeated lookups for the same connection. It handles both IPv4 and IPv6 addresses and includes comprehensive error handling for DNS resolution failures.

## Parameters / Member Variables
- : Pointer to hbaPort structure containing client connection information including IP address and cached DNS resolution results
- DESKTOP-IOASPN6: The hostname pattern from pg_hba.conf to match against the client's resolved hostname

## Dependencies
- Functions called/Symbols referenced:
  - : Performs reverse DNS lookup to resolve IP to hostname
  - : Matches resolved hostname against the configured pattern
  - : Performs forward DNS lookup for verification
  - : Compares IPv4 addresses for equality
  - : Compares IPv6 addresses for equality
  - : Duplicates string in PostgreSQL memory context
  - : Frees address info structures
  - : Logs debug messages
- Called from:
  - : Main HBA authentication checking function in src/backend/libpq/hba.c:2526

## Notes and Other Information
- The function uses a three-state caching system for DNS resolution results: +1 (verified), 0 (not yet resolved), -1 (failed verification), -2 (resolution error)
- DNS resolution failures are cached to avoid repeated expensive DNS operations for known bad hostnames
- The forward DNS verification step is crucial for security - it prevents attacks where an attacker controls reverse DNS but not forward DNS
- Debug messages are logged when address resolution fails to match the original client IP
- The function is static and only used within the HBA authentication module
- Both IPv4 and IPv6 addresses are supported with appropriate comparison functions

## Simplified Source

```c
static bool
check_hostname(hbaPort *port, const char *hostname)
{
    struct addrinfo *gai_result, *gai;
    int ret;
    bool found;

    // Quick exit if hostname resolution previously failed
    if (port->remote_hostname_resolv < 0)
        return false;

    // Perform reverse DNS lookup if not already done
    if (!port->remote_hostname)
    {
        char remote_hostname[NI_MAXHOST];

        ret = pg_getnameinfo_all(&port->raddr.addr, port->raddr.salen,
                                remote_hostname, sizeof(remote_hostname),
                                NULL, 0, NI_NAMEREQD);
        if (ret != 0)
        {
            // Cache failure and return false
            port->remote_hostname_resolv = -2;
            port->remote_hostname_errcode = ret;
            return false;
        }

        port->remote_hostname = pstrdup(remote_hostname);
    }

    // Check if resolved hostname matches the pattern
    if (!hostname_match(hostname, port->remote_hostname))
        return false;

    // If forward lookup already verified, we're done
    if (port->remote_hostname_resolv == +1)
        return true;

    // Perform forward DNS lookup for verification
    ret = getaddrinfo(port->remote_hostname, NULL, NULL, &gai_result);
    if (ret != 0)
    {
        port->remote_hostname_resolv = -2;
        port->remote_hostname_errcode = ret;
        return false;
    }

    // Check if any resolved IP matches the original client IP
    found = false;
    for (gai = gai_result; gai; gai = gai->ai_next)
    {
        if (gai->ai_addr->sa_family == port->raddr.addr.ss_family)
        {
            if (gai->ai_addr->sa_family == AF_INET)
            {
                if (ipv4eq((struct sockaddr_in *) gai->ai_addr,
                          (struct sockaddr_in *) &port->raddr.addr))
                {
                    found = true;
                    break;
                }
            }
            else if (gai->ai_addr->sa_family == AF_INET6)
            {
                if (ipv6eq((struct sockaddr_in6 *) gai->ai_addr,
                          (struct sockaddr_in6 *) &port->raddr.addr))
                {
                    found = true;
                    break;
                }
            }
        }
    }

    // Clean up and cache result
    if (gai_result)
        freeaddrinfo(gai_result);

    if (!found)
        elog(DEBUG2, "hostname \"%s\" rejected - address resolution mismatch", hostname);

    port->remote_hostname_resolv = found ? +1 : -1;
    return found;
}
```