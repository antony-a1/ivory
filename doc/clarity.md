proposed anatomy of ivory
=========================



A top down look at ivory functions and how they relate to each other.

```

*----
everyone else


                  ************************************  +-----------+
                  *            public users          *  |  example  |
                  ************************************  +-----------+
                                   |                          |
                                   v                          v

*----
public interface

                          +-----------+             +-----------+
                          |    cli    | ----------> |    api    |
                          +-----------+             +-----------+
                                                         |
                                                         |  (re-exports only)
                                                         |
                                                    -------------
                                                    v  v  v  v  v


*-----
core functions


                                +-----------+             +------------+
                                | ingestion |             | extraction |
                                +-----------+             +------------+
                                      |                          |
                                      |                          |
                                      |                          |
                                      |                          |
                                      ----------------------------
                                                   |
                                                   |
                                             -------------
                                             v  v  v  v  v

*---------
primitives


                +------------+             +------------+             +------------+
                |dictionaries|             |   stores   |             |    facts   |
                +------------+             +------------+             +------------+
                      |                          |                           |
                      |                          |                           |
                      |                          |                           |
                      |                          |                           |
*------               |---------------------------                           |
storage               |                                                      |
                      |                                                      |
                      |                                                      |
                      v                                                      v
                +-----------+                                         +------------+
                | meta-data |                                         |    data    |
                +-----------+                                         +------------+

```




Mapping this onto a more direct project structure.

 * `ivory-cli`: Command-line tools. Divided into two types of tools, `veneer` and `base`. `veneer` tools include things like
                 ingestion workflows, snapshots, chording, loopback (feature engineering). `base` tools include things like cat fact set,
                edit metadata directly in `$EDITOR`, ad-hoc import/edit of data/stores/dictionaries. _Only_ calls things in `api`.

 * `ivory-api`: Re-exports of public ingest/extract/core/storage functionality only. This is just to help manage public APIs and control consistency.

 * `ivory-ingest`: Apis and tooling for getting data _in to_ an ivory store. Depends on core/storage for heavy lifting. Exposed via api.

 * `ivory-extract`: Apis and tooling for getting data _out of_ an ivory store. Depends on core/storage for heavy lifting. Exposed via api.

 * `ivory-core`: fundamental data structures and interfaces that make ivory a feature store. Stores/Dictionaries/Facts, should depend on ivory data interfaces for
                 actual implementations.

 * `ivory-data`:  metadata, data storage interfaces, abstracts over HDFS/S3/HDFS+S3-SYNC/LOCAL/LOCAL+S3-SYNC/LOCAL+HDFS-SYNC, nothing specific to a feature store.

 * `ivory-example`: Example projects.

 * `ivory-bench`: Benchmarks.

 * `ivory-regression`: Regression test-suite for cross version testing of data/metadata.


Note there are no "generic" buckets here for scoobi/thrift/mr, they will evolve over time, and may stand on their own or be
folded into one of the above.

T.B.D. The core/data/storage distinction needs work and may not be clear until we have a clean meta-data implementation.

Guidelines for APIs between components / layers:

 * Don't expose interface/layer splits based on implementation detail. Basically any difference between HDFS/S3/LOCAL (and those varieties with SYNC steps) needs to be resolved internally to sub-projects, otherwise the nice split doesn't buy as anything. We need to be able to reason about "dictionary import" not "dictionary import on HDFS" or this won't work

 * A follow on to this is, don't export implementation specific effects. For example the storage layer may have a mix of `S3, HDFS` actions internally, but they should be unified to generic structures that don't have a reader of some config blob (this may be `ResultT[IO, _]`, `\/`, `Validation` or any number of things, but importantly lifecycle and control of implementation specific things should not accumulate through interfaces.

 * Don't sacrifice performance for neat interfaces, we need to work out the best granularity of interface to achieve this and it will likely take some trial and error but we need to work towards a mix of safety/performance rather than avoiding it.

 * Always try to support non-hadoop implementations where possible. This helps with testing, usability and performance in the face of small-to-moderate sized data.

 * Any binary format should have adequate tooling. At a minimum a `cat` like tool and an upload tool, but ideally (where data size permits) being able to open in `$EDITOR` or manipulate with command line actions is highly desirable.
