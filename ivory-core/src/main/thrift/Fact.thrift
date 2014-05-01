namespace java com.ambiata.ivory.core.thrift

struct ThriftTombstone {}

union ThriftFactValue {
    1: string s;
    2: i32 i;
    3: i64 l;
    4: double d;
    5: bool b;
    6: ThriftTombstone t;
}

struct ThriftFact {
    1: string entity;
    2: string attribute;
    3: ThriftFactValue value;
    4: optional i32 seconds;
}

struct NamespacedThriftFact {
    1: ThriftFact fact;
    2: string nspace;
    3: i32 yyyyMMdd; // this is a packed int, with a the first 16 bits representing the year, the next 8 the month and the final 8 the day
}
