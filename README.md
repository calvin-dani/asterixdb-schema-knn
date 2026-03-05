



# Vector Database

This repository contains the research prototype accompanying our VLDB publication.  
The implementation is built on top of [Apache AsterixDB](https://asterixdb.apache.org/) and extends it with columnar storage and vector indexing support.

> ⚠️ **Important**  
> This is a research prototype and is **not production-ready**.  
> A reviewed and cleaned version will be made available at:  
> [https://github.com/apache/asterixdb](https://github.com/apache/asterixdb)

---

# Requirements

Before building the project, install:

- **Maven** (3.3.9 or newer)
- **Ansible** (used for cluster deployment)

No additional JVM distributions or code-generation runtimes are required.

---

## What is AsterixDB?

AsterixDB is a BDMS (Big Data Management System) with a rich feature set that sets it apart from other platforms.  Its feature set makes it well-suited to modern needs such as web data warehousing and social data storage and analysis. AsterixDB has:

- __Data model__<br/>
A semistructured NoSQL style data model ([ADM](https://nightlies.apache.org/asterixdb/datamodel.html)) resulting from
extending JSON with object database ideas
- **Query languages**  

- __Query languages__<br/>
An expressive and declarative query language ([SQL++](http://asterixdb.apache.org/docs/0.9.9/sqlpp/manual.html) that supports a broad range of queries and analysis over semistructured data

A parallel runtime query execution engine, Apache Hyracks, that has been scale-tested on up to 1000+ cores and 500+ disks
- **Native storage**  

- __Native storage__<br/>
Partitioned, flexible LSM-based row and column storage, with indexing to support efficient ingestion and management of semistructured data

Support for query access to externally stored data (e.g., data in HDFS) as well as to data stored natively by AsterixDB
- **Data types**  

A rich set of primitive data types, including spatial and temporal data in addition to integer, floating point, and textual data
- **Indexing**  

Secondary indexing options that include B+ trees, R trees, and inverted keyword (exact and fuzzy) index types
- **Transactions**  

Basic transactional (concurrency and recovery) capabilities akin to those of a NoSQL store

Learn more about AsterixDB at its [website](http://asterixdb.apache.org).

## Build from source

To build AsterixDB from source, you should have a platform with the following:

* A Unix-ish environment (Linux, OS X, will all do).
* git
* Maven 3.3.9 or newer.
* JDK 17 or newer.
* Python 3.11+ with pip and venv

Instructions for building the master:

- Checkout AsterixDB master:
  ```
    $git clone https://github.com/apache/asterixdb.git
  ```
- Build AsterixDB master:
  ```
    $cd asterixdb
    $mvn clean package -DskipTests
  ```

## Run the build on your machine

Here are steps to get AsterixDB running on your local machine:

* Start a single-machine AsterixDB instance:

        $cd asterixdb/asterix-server/target/asterix-server-*-binary-assembly/apache-asterixdb-*-SNAPSHOT
        $./opt/local/bin/start-sample-cluster.sh

* Good to go and run queries in your browser at:

        http://localhost:19006

* Read more [documentation](https://nightlies.apache.org/asterixdb/index.html) to learn the data model, query language, and how to create a cluster instance.

## Documentation

To generate the documentation, run asterix-doc with the generate.rr profile in maven, e.g  `mvn -Pgenerate.rr ...`
Be sure to run `mvn package` beforehand or run `mvn site` in asterix-lang-sqlpp to generate some resources that
are used in the documentation that are generated directly from the grammar.

* [master](https://nightlies.apache.org/asterixdb/index.html) |
  [0.9.9](http://asterixdb.apache.org/docs/0.9.9/index.html) |
  [0.9.8](http://asterixdb.apache.org/docs/0.9.8/index.html) |
  [0.9.7](http://asterixdb.apache.org/docs/0.9.7/index.html) |

## Community support

- **Users**  

maling list: [users@asterixdb.apache.org](mailto:users@asterixdb.apache.org)  

Join the list by sending an email to [users-subscribe@asterixdb.apache.org](mailto:users-subscribe@asterixdb.apache.org)  

- **Developers and contributors**  

mailing list:[dev@asterixdb.apache.org](mailto:dev@asterixdb.apache.org)  

Join the list by sending an email to [dev-subscribe@asterixdb.apache.org](mailto:dev-subscribe@asterixdb.apache.org)

