import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from lakebench.datagen import TPCDSDataGenerator


def main() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        inventory_output = root / "inventory"
        TPCDSDataGenerator(
            scale_factor=1,
            target_folder_uri=str(inventory_output),
            target_row_group_size_mb=16,
            table_list=["inventory"],
            multithreading=False,
        ).run()

        inventory_file = inventory_output / "inventory" / "inventory.1.parquet"
        if not inventory_file.is_file():
            raise RuntimeError(f"Expected inventory output is missing: {inventory_file}")
        total_rows = pq.ParquetFile(inventory_file).metadata.num_rows
        if total_rows == 0:
            raise RuntimeError("Smoke test generated no inventory rows.")

        schema_output = root / "schema"
        TPCDSDataGenerator(
            scale_factor=0.01,
            target_folder_uri=str(schema_output),
            target_row_group_size_mb=16,
            table_list=["customer", "reason", "store_sales"],
            multithreading=False,
        ).run()

        customer_schema = pq.read_schema(schema_output / "customer" / "customer.1.parquet")
        if customer_schema.field("c_last_review_date_sk").type != pa.int32():
            raise RuntimeError("customer.c_last_review_date_sk does not have the expected integer type.")

        store_sales_schema = pq.read_schema(schema_output / "store_sales" / "store_sales.1.parquet")
        sales_price_type = store_sales_schema.field("ss_sales_price").type
        if not pa.types.is_decimal(sales_price_type) or sales_price_type.scale != 2:
            raise RuntimeError("store_sales.ss_sales_price does not have the expected decimal scale.")

        reason_rows = pq.ParquetFile(schema_output / "reason" / "reason.1.parquet").metadata.num_rows
        if reason_rows != 75:
            raise RuntimeError(f"C-reference compatibility produced {reason_rows} reason rows instead of 75.")

        print(
            f"Validated {total_rows} inventory rows, automatic part sizing, critical schemas, "
            "and C-reference compatibility."
        )


if __name__ == "__main__":
    main()
