create table customer (
    c_custkey bigint not null, 
    c_name varchar(25) not null, 
    c_address varchar(40) not null, 
    c_nationkey int not null, 
    c_phone char(15) not null, 
    c_acctbal decimal(15, 2) not null, 
    c_mktsegment char(10) not null, 
    c_comment varchar(117) not null
)
partition by (c_mktsegment);

create table nation
  ( n_nationkey     integer not null, 
    n_name          char(25) not null,
    n_regionkey     integer not null,   
    n_comment       varchar(152) not null
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
    l_returnflag    char(1) not null,
    l_linestatus    char(1) not null,
    l_shipdate      date not null,
    l_commitdate    date not null,
    l_receiptdate   date not null,
    l_shipinstruct  char(25) not null,
    l_shipmode      char(10) not null,
	l_comment       varchar(44) not null
)
partition by (l_shipdate);

create table orders
  ( o_orderkey         bigint not null,
    o_custkey          bigint not null,   
    o_orderstatus      char(1) not null,
    o_totalprice       decimal(15,2) not null,
    o_orderdate        date not null,
    o_orderpriority    char(15) not null,
    o_clerk            char(15) not null,
    o_shippriority     integer not null,
    o_comment          varchar(79) not null
)
partition by (o_orderdate);

create table part
  ( p_partkey       bigint not null,      
    p_name          varchar(55) not null,
    p_mfgr          char(25) not null,
    p_brand         char(10) not null,
    p_type          varchar(25) not null,
    p_size          integer not null,
    p_container     char(10) not null,
    p_retailprice   decimal(15,2) not null,
    p_comment       varchar(23) not null
)
partition by (p_brand);

create table partsupp
  ( ps_partkey      bigint not null, 
    ps_suppkey      bigint not null, 
    ps_availqty     integer not null,
    ps_supplycost   decimal(15,2) not null,
    ps_comment      varchar(199) not null
);

create table region
  ( r_regionkey     integer not null,   
    r_name          char(25) not null,
    r_comment       varchar(152) not null
);

create table supplier
  ( s_suppkey       bigint not null,      
    s_name          char(25) not null,
    s_address       varchar(40) not null,
    s_nationkey     integer not null,    
    s_phone         char(15) not null,
    s_acctbal       decimal(15,2) not null,
    s_comment       varchar(101) not null
);
