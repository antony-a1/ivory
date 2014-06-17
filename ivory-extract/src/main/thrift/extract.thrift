namespace java com.ambiata.ivory.extract

struct FactsetLookup {
    1: map<string, i16> priorities;
}

struct PrioritizedFactBytes {
    1: i16 priority;
    2: binary factbytes;
}
