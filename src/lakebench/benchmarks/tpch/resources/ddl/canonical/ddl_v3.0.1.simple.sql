create table customer (
    c_custkey bigint not null, 
    c_name string not null, 
    c_address string not null, 
    c_nationkey int not null, 
    c_phone string not null, 
    c_acctbal decimal(15, 2) not null, 
    c_mktsegment string not null, 
    c_comment string not null
);

create table nation
  ( n_nationkey     integer not null, 
    n_name          string not null,
    n_regionkey     integer not null,   
    n_comment       string not null
);

create table lineitem
  ( l_orderkey      bigint not null,     
    l_partkey       bigint not null,                                           
    l_suppkey       bigint not null,                                         
    l_linenumber    bigint not null,      
    l_quantity      decimal(15,2) not null,
    l_extendedprice decimal(15,2) not null,
    l_discount      decimal(15,2) not null,
    l_tax           decimal(15,2) not null,
    l_returnflag    string not null,
    l_linestatus    string not null,
    l_shipdate      date not null,
    l_commitdate    date not null,
    l_receiptdate   date not null,
    l_shipinstruct  string not null,
    l_shipmode      string not null,
	l_comment       string not null
);

create table orders
  ( o_orderkey         bigint not null,
    o_custkey          bigint not null,   
    o_orderstatus      string not null,
    o_totalprice       decimal(15,2) not null,
    o_orderdate        date not null,
    o_orderpriority    string not null,
    o_clerk            string not null,
    o_shippriority     integer not null,
    o_comment          string not null
);

create table part
  ( p_partkey       bigint not null,      
    p_name          string not null,
    p_mfgr          string not null,
    p_brand         string not null,
    p_type          string not null,
    p_size          integer not null,
    p_container     string not null,
    p_retailprice   decimal(15,2) not null,
    p_comment       string not null
);

create table partsupp
  ( ps_partkey      bigint not null, 
    ps_suppkey      bigint not null, 
    ps_availqty     integer not null,
    ps_supplycost   decimal(15,2) not null,
    ps_comment      string not null
);

create table region
  ( r_regionkey     integer not null,   
    r_name          string not null,
    r_comment       string not null
);

create table supplier
  ( s_suppkey       bigint not null,      
    s_name          string not null,
    s_address       string not null,
    s_nationkey     integer not null,    
    s_phone         string not null,
    s_acctbal       decimal(15,2) not null,
    s_comment       string not null
);
