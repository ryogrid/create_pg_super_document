# print_addr

## Location
[src/tools/ifaddrs/test_ifaddrs.c:18-45](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/ifaddrs/test_ifaddrs.c#L18-L45)

## Overview
A static utility function that converts a socket address structure to a human-readable string representation and prints it to stdout.

## Definition

```c
static void
print_addr(struct sockaddr *addr)
```
## Detailed Description
The  function takes a generic socket address structure and converts it to a human-readable string format using the  system call. It handles both IPv4 and IPv6 addresses by determining the appropriate address family and setting the correct length parameter for the  call. The function outputs the numeric host address (IP address) without performing reverse DNS lookups due to the  flag.

The function is designed as a utility for the ifaddrs testing tool, providing a convenient way to display network interface addresses in a readable format.

## Parameters / Member Variables
- `*addr`: A pointer to a generic socket address structure () that contains the address information to be printed. The actual structure type depends on the address family (IPv4, IPv6, etc.)
## Dependencies
- Functions called/Symbols referenced:
  -  (system call for address-to-name translation)
  -  (standard output function)
- Called from (representative examples):
  -  (at src/tools/ifaddrs/test_ifaddrs.c:49)
  -  (at src/tools/ifaddrs/test_ifaddrs.c:51)

## Notes and Other Information
- The function uses a local buffer of 256 bytes to store the converted address string
- It handles three address families: AF_INET (IPv4), AF_INET6 (IPv6), and defaults to using  size for unknown families
- Uses the  flag to ensure numeric output without DNS resolution, making it faster and more reliable for testing purposes
- If address conversion fails, it prints a descriptive error message showing the unknown address family number
- This is a static function, meaning it's only accessible within the same source file (test_ifaddrs.c)

## Simplified Source

```c
// Simplified version of print_addr
static void print_addr(struct sockaddr *addr) {
    char buffer[256];
    int len;

    // Determine the appropriate length based on address family
    switch (addr->sa_family) {
        case AF_INET:
            len = sizeof(struct sockaddr_in);
            break;
        case AF_INET6:
            len = sizeof(struct sockaddr_in6);
            break;
        default:
            len = sizeof(struct sockaddr_storage);
            break;
    }

    // Convert address to string format
    int ret = getnameinfo(addr, len, buffer, sizeof(buffer), NULL, 0, NI_NUMERICHOST);

    // Print result or error message
    if (ret != 0) {
        printf("[unknown: family %d]", addr->sa_family);
    } else {
        printf("%s", buffer);
    }
}
```

Key simplifications made:
- Added clear comments for each logical section
- Maintained the switch statement for address family handling
- Preserved the getnameinfo call with NI_NUMERICHOST flag
- Kept the error handling for failed address conversion