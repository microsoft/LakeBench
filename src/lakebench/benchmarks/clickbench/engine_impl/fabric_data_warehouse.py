import posixpath
from typing import Optional

from ....engines.fabric_data_warehouse import FabricDataWarehouse
from ....utils.path_utils import abfss_to_https

# ClickBench stores the temporal columns as epoch offsets, so the load rebases
# them onto real DATETIME2 values. Every remaining string column is bounded at
# VARCHAR(8000) to match the engine DDL, which avoids VARCHAR(MAX) columns.
_EPOCH = "'01-01-1970'"
_SECOND_EPOCH_COLUMNS = frozenset(
    {
        "ClientEventTime",
        "EventTime",
        "LocalEventTime",
    }
)
_DAY_EPOCH_COLUMNS = frozenset(
    {
        "EventDate",
    }
)
_VARCHAR_COLUMNS = frozenset(
    {
        "BrowserCountry",
        "BrowserLanguage",
        "FlashMinor2",
        "FromTag",
        "HitColor",
        "MobilePhoneModel",
        "OpenstatAdID",
        "OpenstatCampaignID",
        "OpenstatServiceName",
        "OpenstatSourceID",
        "OriginalURL",
        "PageCharset",
        "ParamCurrency",
        "ParamOrderID",
        "Params",
        "Referer",
        "SearchPhrase",
        "SocialAction",
        "SocialNetwork",
        "SocialSourcePage",
        "Title",
        "URL",
        "UTMCampaign",
        "UTMContent",
        "UTMMedium",
        "UTMSource",
        "UTMTerm",
        "UserAgentMinor",
    }
)
#: Upstream ClickBench column order, which the engine DDL preserves.
_COLUMNS = (
    "WatchID",
    "JavaEnable",
    "Title",
    "GoodEvent",
    "EventTime",
    "EventDate",
    "CounterID",
    "ClientIP",
    "RegionID",
    "UserID",
    "CounterClass",
    "OS",
    "UserAgent",
    "URL",
    "Referer",
    "IsRefresh",
    "RefererCategoryID",
    "RefererRegionID",
    "URLCategoryID",
    "URLRegionID",
    "ResolutionWidth",
    "ResolutionHeight",
    "ResolutionDepth",
    "FlashMajor",
    "FlashMinor",
    "FlashMinor2",
    "NetMajor",
    "NetMinor",
    "UserAgentMajor",
    "UserAgentMinor",
    "CookieEnable",
    "JavascriptEnable",
    "IsMobile",
    "MobilePhone",
    "MobilePhoneModel",
    "Params",
    "IPNetworkID",
    "TraficSourceID",
    "SearchEngineID",
    "SearchPhrase",
    "AdvEngineID",
    "IsArtifical",
    "WindowClientWidth",
    "WindowClientHeight",
    "ClientTimeZone",
    "ClientEventTime",
    "SilverlightVersion1",
    "SilverlightVersion2",
    "SilverlightVersion3",
    "SilverlightVersion4",
    "PageCharset",
    "CodeVersion",
    "IsLink",
    "IsDownload",
    "IsNotBounce",
    "FUniqID",
    "OriginalURL",
    "HID",
    "IsOldCounter",
    "IsEvent",
    "IsParameter",
    "DontCountHits",
    "WithHash",
    "HitColor",
    "LocalEventTime",
    "Age",
    "Sex",
    "Income",
    "Interests",
    "Robotness",
    "RemoteIP",
    "WindowName",
    "OpenerName",
    "HistoryLength",
    "BrowserLanguage",
    "BrowserCountry",
    "SocialNetwork",
    "SocialAction",
    "HTTPError",
    "SendTiming",
    "DNSTiming",
    "ConnectTiming",
    "ResponseStartTiming",
    "ResponseEndTiming",
    "FetchTiming",
    "SocialSourceNetworkID",
    "SocialSourcePage",
    "ParamPrice",
    "ParamOrderID",
    "ParamCurrency",
    "ParamCurrencyID",
    "OpenstatServiceName",
    "OpenstatCampaignID",
    "OpenstatAdID",
    "OpenstatSourceID",
    "UTMSource",
    "UTMMedium",
    "UTMCampaign",
    "UTMContent",
    "UTMTerm",
    "FromTag",
    "HasGCLID",
    "RefererHash",
    "URLHash",
    "CLID",
)


def _projection(column_name: str) -> str:
    if column_name in _SECOND_EPOCH_COLUMNS:
        return f"DATEADD(SECOND, {column_name}, {_EPOCH}) AS {column_name}"
    if column_name in _DAY_EPOCH_COLUMNS:
        return f"DATEADD(DAY, {column_name}, {_EPOCH}) AS {column_name}"
    if column_name in _VARCHAR_COLUMNS:
        return f"CAST({column_name} AS varchar(8000)) AS {column_name}"
    return column_name


class FabricDataWarehouseClickBench:
    def __init__(self, engine: FabricDataWarehouse):
        self.engine = engine

    def load_parquet_to_delta(
        self,
        table_name: str,
        parquet_folder_uri: str,
        table_is_precreated: bool = True,
        context_decorator: Optional[str] = None,
    ):
        """
        Load the ClickBench Parquet data into the warehouse using OPENROWSET.

        Parameters
        ----------
        table_name : str
            Name of the target table to insert data into.
        parquet_folder_uri : str
            Path to the source Parquet files.
        table_is_precreated : bool, default True
            Whether the target table already exists.
        context_decorator : str, optional
            Label applied to the statement so it can be correlated in Fabric telemetry.
        """
        parquet_glob = posixpath.join(abfss_to_https(parquet_folder_uri), "*.parquet")
        projection = ",\n                ".join(_projection(column_name) for column_name in _COLUMNS)

        return self.engine.execute_sql_statement(
            f"""
            INSERT INTO {self.engine.schema_name}.{table_name}
            SELECT
                {projection}
            FROM OPENROWSET(BULK '{parquet_glob}')
            """,
            context_decorator=context_decorator,
        )

    def execute_sql_query(self, query: str, context_decorator: Optional[str] = None):
        return self.engine.execute_sql_query(query, context_decorator=context_decorator)
