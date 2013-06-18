Graph Databases Use Cases
=========================

Example use case implementations from the O'Reilly book [Graph Databases](http://graphdatabases.com/) by [@iansrobinson](http://twitter.com/iansrobinson), [@jimwebber](http://twitter.com/jimwebber) and [@emileifrem](http://twitter.com/emileifrem).

Setup
-----

This repository contains a submodule, _neode_, which is used to build the performance datasets. After cloning the repository, you will need to initialize the submodule:

    git submodule init

and then:

    git submodule update

To run the use case queries:

    mvn clean install

Overview
--------

Queries are developed in a test-driven fashion against small, well-known representative graphs (as described pp.83-87 of the book). The queries can then be run against a much larger, randomly-generated graph (typically, 1-2 million nodes and several million relationships), to test their relative performance. (Note: these performance tests do not test production-like scenarios; rather, they act as a sanity check, ensuring that queries that run fast against a very small graph are still reasonably performant when run against a larger graph.)

The project contains 3 modules (in addition to the _neode_ submodule):

*  _queries_

   Contains the use case queries and the unit tests used to develop the queries.
* _dataset_builders_

   Builds larger, randomly-generated sample datasets.
* _performance_tests_

   Runs the queries against the large sample datasets.

Running the Performance Tests
-----------------------------

First, build the project as described in Setup.

Before you run the performance tests you will need to generate sample datasets. To create a sample dataset run:

    mvn test -pl data-generation -DargLine="-Xms2g -Xmx2g" -Dtest=AccessControl|Logistics|SocialNetwork

For example, to generate a sample dataset for the Logistics queries, run:

    mvn test -pl data-generation -DargLine="-Xms2g -Xmx2g" -Dtest=Logistics

*WARNING:* Building the sample datasets takes a long time (several tens of minutes in some cases).

To execute the performance tests against a sample dataset, run:

    mvn test -pl performance-testing -DargLine="-Xms2g -Xmx2g" -Dtest=AccessControl|Logistics|SocialNetwork
