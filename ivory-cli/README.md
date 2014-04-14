 ivory-cli
=========


### Primary Commands

```

ivory create-repository [-r|--repository REPOSITORY_URI]

ivory import-dictionary [-r|--repository REPOSITORY_URI] DICTIONARY_PATH

ivory import-facts [-r|--repository REPOSITORY_URI] FACT_PATH

ivory tag [-r|--repository REPOSITORY_URI] [-a|--at VERSION] TAG

ivory extract [-r|--repository REPOSITORY_URI] [-a|--at VERSION] [-t|--tag TAG] [DATE]

```

### Manual maintenance tools

```

ivory store-add [-r|--repository REPOSITORY_URI] FACT_SET_PATH ...

ivory store-remove [-r|--repository REPOSITORY_URI] FACT_SET_PATH ...

```


### Meta VARs

```
REPOSITORY_URI, one of the following explicit protocols (no protocol implies local):
 - `hdfs:`
 - `s3:`
 - `local:`

TAG, alpha-numeric-hyphen humanized identifier

VERSION, ivory internally generated version

DATE, an explicit date, defaults to today

*_PATH, paths to some sort of dataset

```
