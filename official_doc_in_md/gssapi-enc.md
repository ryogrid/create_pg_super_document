18.10. Secure TCP/IP Connections with GSSAPI Encryption  
---  
[Prev](ssl-tcp.md "18.9. Secure TCP/IP Connections with SSL") | [Up](runtime.md "Chapter 18. Server Setup and Operation")| Chapter 18. Server Setup and Operation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](ssh-tunnels.md "18.11. Secure TCP/IP Connections with SSH Tunnels")  
  
* * *

## 18.10. Secure TCP/IP Connections with GSSAPI Encryption #

[18.10.1. Basic Setup](gssapi-enc.md#GSSAPI-SETUP)

PostgreSQL also has native support for using GSSAPI to encrypt client/server communications for increased security. Support requires that a GSSAPI implementation (such as MIT Kerberos) is installed on both client and server systems, and that support in PostgreSQL is enabled at build time (see [Chapter 17](installation.md "Chapter 17. Installation from Source Code")). 

### 18.10.1. Basic Setup #

The PostgreSQL server will listen for both normal and GSSAPI-encrypted connections on the same TCP port, and will negotiate with any connecting client whether to use GSSAPI for encryption (and for authentication). By default, this decision is up to the client (which means it can be downgraded by an attacker); see [Section 20.1](auth-pg-hba-conf.md "20.1. The pg_hba.conf File") about setting up the server to require the use of GSSAPI for some or all connections. 

When using GSSAPI for encryption, it is common to use GSSAPI for authentication as well, since the underlying mechanism will determine both client and server identities (according to the GSSAPI implementation) in any case. But this is not required; another PostgreSQL authentication method can be chosen to perform additional verification. 

Other than configuration of the negotiation behavior, GSSAPI encryption requires no setup beyond that which is necessary for GSSAPI authentication. (For more information on configuring that, see [Section 20.6](gssapi-auth.md "20.6. GSSAPI Authentication").) 

* * *

[Prev](ssl-tcp.md "18.9. Secure TCP/IP Connections with SSL") | [Up](runtime.md "Chapter 18. Server Setup and Operation")|  [Next](ssh-tunnels.md "18.11. Secure TCP/IP Connections with SSH Tunnels")  
---|---|---  
18.9. Secure TCP/IP Connections with SSL | [Home](index.md "PostgreSQL 17.5 Documentation")|  18.11. Secure TCP/IP Connections with SSH Tunnels
