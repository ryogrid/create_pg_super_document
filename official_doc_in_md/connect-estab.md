50.2. How Connections Are Established  
---  
[Prev](query-path.md "50.1. The Path of a Query") | [Up](overview.md "Chapter 50. Overview of PostgreSQL Internals")| Chapter 50. Overview of PostgreSQL Internals| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](parser-stage.md "50.3. The Parser Stage")  
  
* * *

## 50.2. How Connections Are Established #

PostgreSQL implements a “process per user” client/server model. In this model, every [_[client process](glossary.md#GLOSSARY-CLIENT "Client \(process\)")_](glossary.md#GLOSSARY-CLIENT) connects to exactly one [_[backend process](glossary.md#GLOSSARY-BACKEND "Backend \(process\)")_](glossary.md#GLOSSARY-BACKEND). As we do not know ahead of time how many connections will be made, we have to use a “supervisor process” that spawns a new backend process every time a connection is requested. This supervisor process is called [_[postmaster](glossary.md#GLOSSARY-POSTMASTER "Postmaster \(process\)")_](glossary.md#GLOSSARY-POSTMASTER) and listens at a specified TCP/IP port for incoming connections. Whenever it detects a request for a connection, it spawns a new backend process. Those backend processes communicate with each other and with other processes of the [_[instance](glossary.md#GLOSSARY-INSTANCE "Instance")_](glossary.md#GLOSSARY-INSTANCE) using _semaphores_ and [_[shared memory](glossary.md#GLOSSARY-SHARED-MEMORY "Shared memory")_](glossary.md#GLOSSARY-SHARED-MEMORY) to ensure data integrity throughout concurrent data access. 

The client process can be any program that understands the PostgreSQL protocol described in [Chapter 53](protocol.md "Chapter 53. Frontend/Backend Protocol"). Many clients are based on the C-language library libpq, but several independent implementations of the protocol exist, such as the Java JDBC driver. 

Once a connection is established, the client process can send a query to the backend process it's connected to. The query is transmitted using plain text, i.e., there is no parsing done in the client. The backend process parses the query, creates an _execution plan_ , executes the plan, and returns the retrieved rows to the client by transmitting them over the established connection. 

* * *

[Prev](query-path.md "50.1. The Path of a Query") | [Up](overview.md "Chapter 50. Overview of PostgreSQL Internals")|  [Next](parser-stage.md "50.3. The Parser Stage")  
---|---|---  
50.1. The Path of a Query | [Home](index.md "PostgreSQL 17.5 Documentation")|  50.3. The Parser Stage
