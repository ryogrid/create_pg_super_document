32.18. LDAP Lookup of Connection Parameters  
---  
[Prev](libpq-pgservice.md "32.17. The Connection Service File") | [Up](libpq.md "Chapter 32. libpq — C Library")| Chapter 32. libpq — C Library| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](libpq-ssl.md "32.19. SSL Support")  
  
* * *

## 32.18. LDAP Lookup of Connection Parameters #

If libpq has been compiled with LDAP support (option ``\--with-ldap`` for `configure`) it is possible to retrieve connection options like `host` or `dbname` via LDAP from a central server. The advantage is that if the connection parameters for a database change, the connection information doesn't have to be updated on all client machines. 

LDAP connection parameter lookup uses the connection service file `pg_service.conf` (see [Section 32.17](libpq-pgservice.md "32.17. The Connection Service File")). A line in a `pg_service.conf` stanza that starts with `ldap://` will be recognized as an LDAP URL and an LDAP query will be performed. The result must be a list of `keyword = value` pairs which will be used to set connection options. The URL must conform to [RFC 1959](https://datatracker.ietf.org/doc/html/rfc1959) and be of the form 
    
    
    ldap://[_hostname_[:_port_]]/_search_base_?_attribute_?_search_scope_?_filter_
    

where _`hostname`_ defaults to `localhost` and _`port`_ defaults to 389. 

Processing of `pg_service.conf` is terminated after a successful LDAP lookup, but is continued if the LDAP server cannot be contacted. This is to provide a fallback with further LDAP URL lines that point to different LDAP servers, classical `keyword = value` pairs, or default connection options. If you would rather get an error message in this case, add a syntactically incorrect line after the LDAP URL. 

A sample LDAP entry that has been created with the LDIF file 
    
    
    version:1
    dn:cn=mydatabase,dc=mycompany,dc=com
    changetype:add
    objectclass:top
    objectclass:device
    cn:mydatabase
    description:host=dbserver.mycompany.com
    description:port=5439
    description:dbname=mydb
    description:user=mydb_user
    description:sslmode=require
    

might be queried with the following LDAP URL: 
    
    
    ldap://ldap.mycompany.com/dc=mycompany,dc=com?description?one?(cn=mydatabase)
    

You can also mix regular service file entries with LDAP lookups. A complete example for a stanza in `pg_service.conf` would be: 
    
    
    # only host and port are stored in LDAP, specify dbname and user explicitly
    [customerdb]
    dbname=customer
    user=appuser
    ldap://ldap.acme.com/cn=dbserver,cn=hosts?pgconnectinfo?base?(objectclass=*)
    

* * *

[Prev](libpq-pgservice.md "32.17. The Connection Service File") | [Up](libpq.md "Chapter 32. libpq — C Library")|  [Next](libpq-ssl.md "32.19. SSL Support")  
---|---|---  
32.17. The Connection Service File | [Home](index.md "PostgreSQL 17.5 Documentation")|  32.19. SSL Support
