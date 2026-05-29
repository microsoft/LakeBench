-- TPC-DI v1.1.0 Target Data Warehouse DDL (SparkSQL dialect)
-- Staging Tables

CREATE OR REPLACE TABLE staging_status_type (
    st_id STRING,
    st_name STRING
);

CREATE OR REPLACE TABLE staging_tax_rate (
    tx_id STRING,
    tx_name STRING,
    tx_rate DECIMAL(6,5)
);

CREATE OR REPLACE TABLE staging_trade_type (
    tt_id STRING,
    tt_name STRING,
    tt_is_sell INT,
    tt_is_mrkt INT
);

CREATE OR REPLACE TABLE staging_industry (
    in_id STRING,
    in_name STRING,
    in_sc_id STRING
);

CREATE OR REPLACE TABLE staging_hr (
    employee_id INT,
    manager_id INT,
    employee_first_name STRING,
    employee_last_name STRING,
    employee_mi STRING,
    employee_job_code STRING,
    employee_branch STRING,
    employee_office STRING,
    employee_phone STRING
);

CREATE OR REPLACE TABLE staging_prospect (
    agency_id STRING,
    last_name STRING,
    first_name STRING,
    middle_initial STRING,
    gender STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state STRING,
    country STRING,
    phone STRING,
    income INT,
    number_cars INT,
    number_children INT,
    marital_status STRING,
    age INT,
    credit_rating INT,
    own_or_rent_flag STRING,
    employer STRING,
    number_credit_cards INT,
    net_worth INT
);

CREATE OR REPLACE TABLE staging_daily_market (
    dm_date DATE,
    dm_s_symb STRING,
    dm_close DECIMAL(8,2),
    dm_high DECIMAL(8,2),
    dm_low DECIMAL(8,2),
    dm_vol INT
);

CREATE OR REPLACE TABLE staging_watch_history (
    w_c_id BIGINT,
    w_s_symb STRING,
    w_dts TIMESTAMP,
    w_action STRING
);

CREATE OR REPLACE TABLE staging_trade (
    t_id BIGINT,
    t_dts TIMESTAMP,
    t_st_id STRING,
    t_tt_id STRING,
    t_is_cash INT,
    t_s_symb STRING,
    t_qty INT,
    t_bid_price DECIMAL(8,2),
    t_ca_id BIGINT,
    t_exec_name STRING,
    t_trade_price DECIMAL(8,2),
    t_chrg DECIMAL(10,2),
    t_comm DECIMAL(10,2),
    t_tax DECIMAL(10,2)
);

CREATE OR REPLACE TABLE staging_trade_history (
    th_t_id BIGINT,
    th_dts TIMESTAMP,
    th_st_id STRING
);

CREATE OR REPLACE TABLE staging_cash_transaction (
    ct_ca_id BIGINT,
    ct_dts TIMESTAMP,
    ct_amt DECIMAL(10,2),
    ct_name STRING
);

CREATE OR REPLACE TABLE staging_customer (
    cdc_flag STRING,
    cdc_dsn BIGINT,
    c_id BIGINT,
    c_tax_id STRING,
    c_st_id STRING,
    c_l_name STRING,
    c_f_name STRING,
    c_m_name STRING,
    c_gndr STRING,
    c_tier SMALLINT,
    c_dob DATE,
    c_adline1 STRING,
    c_adline2 STRING,
    c_zipcode STRING,
    c_city STRING,
    c_state_prov STRING,
    c_ctry STRING,
    c_ctry_1 STRING,
    c_area_1 STRING,
    c_local_1 STRING,
    c_ext_1 STRING,
    c_ctry_2 STRING,
    c_area_2 STRING,
    c_local_2 STRING,
    c_ext_2 STRING,
    c_ctry_3 STRING,
    c_area_3 STRING,
    c_local_3 STRING,
    c_ext_3 STRING,
    c_email_1 STRING,
    c_email_2 STRING,
    c_lcl_tx_id STRING,
    c_nat_tx_id STRING
);

CREATE OR REPLACE TABLE staging_account (
    cdc_flag STRING,
    cdc_dsn BIGINT,
    ca_id BIGINT,
    ca_b_id BIGINT,
    ca_c_id BIGINT,
    ca_name STRING,
    ca_tax_st SMALLINT,
    ca_st_id STRING
);

CREATE OR REPLACE TABLE staging_finwire_cmp (
    pts TIMESTAMP,
    rec_type STRING,
    company_name STRING,
    cik BIGINT,
    status STRING,
    industry_id STRING,
    sp_rating STRING,
    founding_date DATE,
    addr_line1 STRING,
    addr_line2 STRING,
    postal_code STRING,
    city STRING,
    state_province STRING,
    country STRING,
    ceo_name STRING,
    description STRING
);

CREATE OR REPLACE TABLE staging_finwire_sec (
    pts TIMESTAMP,
    rec_type STRING,
    symbol STRING,
    issue_type STRING,
    status STRING,
    name STRING,
    ex_id STRING,
    sh_out BIGINT,
    first_trade_date DATE,
    first_trade_exchange DATE,
    dividend DECIMAL(10,2),
    co_name_or_cik STRING
);

CREATE OR REPLACE TABLE staging_finwire_fin (
    pts TIMESTAMP,
    rec_type STRING,
    year INT,
    quarter SMALLINT,
    qtr_start_date DATE,
    posting_date DATE,
    revenue DECIMAL(15,2),
    earnings DECIMAL(15,2),
    eps DECIMAL(10,2),
    diluted_eps DECIMAL(10,2),
    margin DECIMAL(10,2),
    inventory DECIMAL(15,2),
    assets DECIMAL(15,2),
    liabilities DECIMAL(15,2),
    sh_out BIGINT,
    diluted_sh_out BIGINT,
    co_name_or_cik STRING
);

-- Dimension Tables

CREATE OR REPLACE TABLE dim_date (
    sk_date_id BIGINT,
    date_value DATE,
    date_desc STRING,
    calendar_year_id SMALLINT,
    calendar_year_desc STRING,
    calendar_qtr_id SMALLINT,
    calendar_qtr_desc STRING,
    calendar_month_id SMALLINT,
    calendar_month_desc STRING,
    calendar_week_id SMALLINT,
    calendar_week_desc STRING,
    day_of_week_num SMALLINT,
    day_of_week_desc STRING,
    fiscal_year_id SMALLINT,
    fiscal_year_desc STRING,
    fiscal_qtr_id SMALLINT,
    fiscal_qtr_desc STRING,
    holiday_flag BOOLEAN
);

CREATE OR REPLACE TABLE dim_time (
    sk_time_id BIGINT,
    time_value STRING,
    hour_id SMALLINT,
    hour_desc STRING,
    minute_id SMALLINT,
    minute_desc STRING,
    second_id SMALLINT,
    second_desc STRING,
    market_hours_flag BOOLEAN,
    office_hours_flag BOOLEAN
);

CREATE OR REPLACE TABLE dim_status_type (
    st_id STRING,
    st_name STRING
);

CREATE OR REPLACE TABLE dim_tax_rate (
    tx_id STRING,
    tx_name STRING,
    tx_rate DECIMAL(6,5)
);

CREATE OR REPLACE TABLE dim_trade_type (
    tt_id STRING,
    tt_name STRING,
    tt_is_sell INT,
    tt_is_mrkt INT
);

CREATE OR REPLACE TABLE dim_broker (
    sk_broker_id BIGINT,
    broker_id BIGINT,
    manager_id BIGINT,
    first_name STRING,
    last_name STRING,
    middle_initial STRING,
    branch STRING,
    office STRING,
    phone STRING,
    is_current BOOLEAN,
    batch_id INT,
    effective_date DATE,
    end_date DATE
);

CREATE OR REPLACE TABLE dim_customer (
    sk_customer_id BIGINT,
    customer_id BIGINT,
    tax_id STRING,
    status STRING,
    last_name STRING,
    first_name STRING,
    middle_name STRING,
    gender STRING,
    tier SMALLINT,
    dob DATE,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_province STRING,
    country STRING,
    phone1 STRING,
    phone2 STRING,
    phone3 STRING,
    email1 STRING,
    email2 STRING,
    national_tx_id STRING,
    national_tx_desc STRING,
    national_tx_rate DECIMAL(6,5),
    local_tx_id STRING,
    local_tx_desc STRING,
    local_tx_rate DECIMAL(6,5),
    agency_id STRING,
    credit_rating INT,
    net_worth INT,
    marketing_nameplate STRING,
    is_current BOOLEAN,
    batch_id INT,
    effective_date DATE,
    end_date DATE
);

CREATE OR REPLACE TABLE dim_account (
    sk_account_id BIGINT,
    account_id BIGINT,
    sk_broker_id BIGINT,
    sk_customer_id BIGINT,
    account_desc STRING,
    tax_status SMALLINT,
    status STRING,
    is_current BOOLEAN,
    batch_id INT,
    effective_date DATE,
    end_date DATE
);

CREATE OR REPLACE TABLE dim_company (
    sk_company_id BIGINT,
    company_id BIGINT,
    status STRING,
    name STRING,
    industry STRING,
    sp_rating STRING,
    is_low_grade BOOLEAN,
    ceo STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_province STRING,
    country STRING,
    description STRING,
    founding_date DATE,
    is_current BOOLEAN,
    batch_id INT,
    effective_date DATE,
    end_date DATE
);

CREATE OR REPLACE TABLE dim_security (
    sk_security_id BIGINT,
    symbol STRING,
    issue_type STRING,
    status STRING,
    name STRING,
    exchange_id STRING,
    sk_company_id BIGINT,
    shares_outstanding BIGINT,
    first_trade DATE,
    first_trade_on_exchange DATE,
    dividend DECIMAL(10,2),
    is_current BOOLEAN,
    batch_id INT,
    effective_date DATE,
    end_date DATE
);

CREATE OR REPLACE TABLE dim_trade (
    sk_trade_id BIGINT,
    trade_id BIGINT,
    sk_broker_id BIGINT,
    sk_create_date_id BIGINT,
    sk_create_time_id BIGINT,
    sk_close_date_id BIGINT,
    sk_close_time_id BIGINT,
    status STRING,
    type STRING,
    is_cash BOOLEAN,
    sk_security_id BIGINT,
    sk_company_id BIGINT,
    quantity INT,
    bid_price DECIMAL(8,2),
    sk_customer_id BIGINT,
    sk_account_id BIGINT,
    executed_by STRING,
    trade_price DECIMAL(8,2),
    fee DECIMAL(10,2),
    commission DECIMAL(10,2),
    tax DECIMAL(10,2),
    batch_id INT
);

-- Fact Tables

CREATE OR REPLACE TABLE fact_market_history (
    sk_security_id BIGINT,
    sk_company_id BIGINT,
    sk_date_id BIGINT,
    peratio DECIMAL(10,2),
    yield_val DECIMAL(5,2),
    fifty_two_week_high DECIMAL(8,2),
    sk_fifty_two_week_high_date BIGINT,
    fifty_two_week_low DECIMAL(8,2),
    sk_fifty_two_week_low_date BIGINT,
    close_price DECIMAL(8,2),
    day_high DECIMAL(8,2),
    day_low DECIMAL(8,2),
    volume INT,
    batch_id INT
);

CREATE OR REPLACE TABLE fact_watches (
    sk_customer_id BIGINT,
    sk_security_id BIGINT,
    sk_date_id_date_placed BIGINT,
    sk_date_id_date_removed BIGINT,
    batch_id INT
);

CREATE OR REPLACE TABLE fact_cash_balances (
    sk_customer_id BIGINT,
    sk_account_id BIGINT,
    sk_date_id BIGINT,
    cash DECIMAL(15,2),
    batch_id INT
);

CREATE OR REPLACE TABLE fact_holdings (
    trade_id BIGINT,
    current_trade_id BIGINT,
    sk_customer_id BIGINT,
    sk_account_id BIGINT,
    sk_security_id BIGINT,
    sk_company_id BIGINT,
    sk_date_id BIGINT,
    sk_time_id BIGINT,
    current_price DECIMAL(8,2),
    current_holding INT,
    batch_id INT
);

-- Other Tables

CREATE OR REPLACE TABLE financial (
    sk_company_id BIGINT,
    fi_year INT,
    fi_qtr SMALLINT,
    fi_qtr_start_date DATE,
    fi_revenue DECIMAL(15,2),
    fi_net_earn DECIMAL(15,2),
    fi_basic_eps DECIMAL(10,2),
    fi_dilut_eps DECIMAL(10,2),
    fi_margin DECIMAL(10,2),
    fi_inventory DECIMAL(15,2),
    fi_assets DECIMAL(15,2),
    fi_liability DECIMAL(15,2),
    fi_out_basic BIGINT,
    fi_out_dilut BIGINT
);

CREATE OR REPLACE TABLE prospect (
    agency_id STRING,
    sk_record_date_id BIGINT,
    sk_update_date_id BIGINT,
    batch_id INT,
    is_customer BOOLEAN,
    last_name STRING,
    first_name STRING,
    middle_initial STRING,
    gender STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state STRING,
    country STRING,
    phone STRING,
    income INT,
    number_cars INT,
    number_children INT,
    marital_status STRING,
    age INT,
    credit_rating INT,
    own_or_rent_flag STRING,
    employer STRING,
    number_credit_cards INT,
    net_worth INT,
    marketing_nameplate STRING
);

-- Audit Table

CREATE OR REPLACE TABLE di_messages (
    message_date_and_time TIMESTAMP,
    batch_id INT,
    message_source STRING,
    message_text STRING,
    message_type STRING,
    message_data STRING
);
