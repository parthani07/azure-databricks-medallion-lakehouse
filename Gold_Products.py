#DLT Pipeline

#Import and expectations

import dlt
from pyspark.sql.functions import *

#Expectations
my_rules={
    "rule1":"product_id IS NOT NULL",
    "rule2":"product_name IS NOT NULL"
}

#Streaming table

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def dimproducts_stage():
    return (
        spark.readStream.format("delta").table("databricks_cata.silver.products_silver")
    )


#Streaming view

@dlt.view()
def DimProducts_view():
    df = spark.readStream.table("LIVE.DimProducts_stage")
    return df

#DimProducts

dlt.create_streaming_table("DimProducts")

dlt.apply_changes(
    target="DimProducts",
    source="LIVE.DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)
