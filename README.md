ivory
=====

```
ivory: (the ivories) the keys of a piano
```

Overview
--------

*ivory* defines a specification for how to store feature data and provides a set of tools for
querying it. It does not provide any tooling for producing feature data in the first place.
All ivory commands run as MapReduce jobs so it assumed that feature data is maintained on HDFS.


Repository
----------

The tooling provided operates on an *ivory repository*. An ivory repository is a convention for
storing *fact sets*, feature *dictionaries* and *feature stores*. The directory structure is
as follows:

```
ivory_repository/
├── metadata
│   ├── dictionaries
│   │   ├── dictionary1
│   │   └── dictionary2
│   └── stores
│       ├── feature_store1
│       └── feature_store2
└── fact_sets
    ├── fact_set1
    └── fact_set2
```


Fact Sets
---------

A *fact set* is a single directory containing multiple *facts*, where a *fact* defines:

1. The *entity* the feature value is associated with;
2. An *attribute* specifying which feature;
4. The *value* itself;
3. The *time* from which the feature value is valid.

That is, a fact is simply an *EAVT* record. Multiple facts form a fact set, which is described within
*EAVT* files that are partitioned by *namespace* and *date*. For example:

```
my_fact_set/
├── widgets
│   └── 2014
│       └── 01
│           ├── 09
│           │   ├── eavt-00000
│           │   ├── eavt-00001
│           │   └── eavt-00002
│           ├── 10
│           │   ├── eavt-00000
│           │   ├── eavt-00001
│           │   └── eavt-00002
│           └── 11
│               ├── eavt-00000
│               ├── eavt-00001
│               └── eavt-00002
└── demo
    └── 2013
        └── 01
            └── 09
                ├── eavt-00000
                └── eavt-00001

```

In this fact set, facts are partioned across two namespaces: `widgets` and `demo`. The *widget* facts
are spread accross three dates, while *demographic* facts are constrained to one. Note also that
a given namespace-partition can contain multiple EAVT files.

EAVT files are simply pipe-delimited text files with one EAVT record per line. For example, a line in
the file `my_fact_set/widgets/2013/01/10/eavt-00001` might look like:

```
928340|inbound.count.1W|35|43200
```

That is, the fact: "feature attribute `inbound.count.1W` has value `35` for entity 928340 as of
10/01/2014 12:00". The time component of the record is the number of seconds into the day specified by
the partition the record belongs to. Note that Ivory does not enforce or specify a time zone for the time component
of a fact. A time zone should be chosen that is reflective of the domain, however, for a given Ivory
feature store, the time zone for all facts should be the same.


Feature store
-------------

A feature store is comprised of one or more *fact sets*, which is represented by a text file containing
an ordered list of references to fact sets. For example:

```
00005
00004
00003
widget_fixed
00002
00001
00000
```

The ordering is important as it allows facts to be overriden. When a feature store is queried, if multiple facts
with the same entity, attribute and time are identified, the value from the fact contained in the most recent fact
set will be used, where most recent means listed higher in the feature store file.

Because a feature store can be speified by just referencing fact sets, Ivory can support poor-man versioning giving
rise to use cases such as:

* overrding buggy values with corrected ones;
* combining *production* features with *ad-hoc* features.


Dictionary
----------

All features are identified by their name and namespace. In the example fact above, the feature is `widgets:inbound.count.1W`
where `widgets` is the namespace and `inbound.count.1W` is the name. With Ivory we must also associate with any namespace-name
feature identifier the following metadata:

* An *encoding* specifying the type encoding of a feature value:
    * `boolean`
    * `int`
    * `double`
    * `string`

* A *classification* type specifying how a feature value can be semantically interpreted and used:
    * `numerical`
    * `categorical`

* A human-readable *description*.

In Ivory, feature metadata is seperated from the features store (facts) in its own set of text files known
as *feature dictionaries*. Dictionary text files are also pipe-delimited and of the following form:

```
namespace|name|encoding|type|description
```

So for the fact above, we could specify a dictionary entry such as:

```
widgets|inbound.count.1W|int|numerical|Count in the last week
```

Other dictionary entries might look like the following:

```
demo|gender|string|categorical|Gender
demo|postcode|string|categorical|Postcode
```

Given a dictionary, we can use Ivory to validate facts in a feature store. The `validate` command will
check that the encoding types specified for features in the dictionary are consistent with facts:

```
> ivory validate --feature-store feature_store.txt --dictionary feature_dictionary.txt
```

We can also use Ivory to generate statistics for the values of specific features accross a feature store using the
`inspect` command. This will compute statistics such as density, ranges (for numerical features), factors (for
categorical features), historgrams, means, etc. Inspections can filter both the features of interest as well which
facts to considered by time:

```
> ivory inspect --feature-store feature_store.txt --dictionary feature_dictionary.txt --features features.txt --start-time '2013-01-01' --end-time '2014-01-01'
```


Querying
--------

Ivory supports two types of queries: *snapshots* and *chords*.


A `snaphot` query is used to extract the feature values for entities at a certain point in time. Snapshoting can filter
the set of features and/or entities considered. By default the output is in *EAVT* format, but can be output in
row-oriented form (i.e. column per feature) using the `--pivot` option. When a  snapshot` query is performed, the most
recent feature value for a given entity-attribute, with respect to the snapshot time, will be returned in the output:

```
# get a snapshot of values for specific features and entities as of 1 Nov 2013
> ivory snapshot --feature-store feature_store.txt --dictionary feature_dictionary.txt --features features.txt --entities entities.txt --time '2013-11-01' --output nov2013snapshot

# Pivot the table to be row oriented
> ivory snapshot --pivot --feature-store feature_store.txt --dictionary feature_dictionary.txt --features features.txt --entities entities.txt --time '2013-11-01' --output nov2013snapshot
```

A `chord` query is used to extract the feature values for entities at different points in time - *instances*. That is, for each entity, a
different time is specified. In fact, multiple times can be specified per entity. To invoke a chord query, a file of *instance
descriptors* must be specified that are entity-time pairs, for example:

```
928340|2013-11-01
928340|2013-11-08
928316|2013-11-08
928316|2013-11-15
```

Like `snapshot`, `chord` by default will output in *EAVT* format, but can be output in row-oriented form using the `--pivot` option:

```
> ivory chord --feature-store feature_store.txt --dictionary feature_dictionary.txt --instances instances.txt --output nov2013snapshot

> ivory chord --pivot --feature-store feature_store.txt --dictionary feature_dictionary.txt --instances instances.txt --output nov2013snapshot
```


Data Generation
---------------

Ivory supports generating random dictionaries and fact sets which can be used for testing.

To generate a random dictionary, you need to specify the number of features, and an output location:

```
> ivory generate-dictionary --features 10000 --output random_dictionary
```

This outputs two files:

1. the dictionary itself.
2. a feature flag file specifying the sparcity and frequency of each feature, where sparcity is a double between 0.0 and 1.0, and frequency is one of `daily`, `weekly`, or `monthly`.

The format of the feature flag file is:

```
namespace|name|sparcity|fequency
```

An example is:
```
widgets|inbound.count.1W|0.4|weekly
demo|postcode|0.7|monthly
```

To generate random facts, you need to specify a dictionary, feature flag file, number of entities, time range, number of fact sets, and output location:

```
> ivory generate-facts --dictionary feature_dictionary.txt --flags feature_flags.txt --entities 10000000 --time-range 2012-01-01_to_2012-12-31 --factsets 3 --output random_factsets
```

This outputs a fact set partitioned by *namespace* and *date* so it can be used as part of a feature store.



Versioning
----------

The format of fact sets are versioned. This allows the format of fact sets to be modified in the future but still maintain feature stores that
reference fact sets persisted in an older format.

A fact set format version is specifed by a `.version` file that is stored at the root directory of a given fact set.
