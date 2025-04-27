from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
import uvicorn
from pathlib import Path
import os

app = FastAPI()
spark = None

HUDI_TABLES = {}

def discover_hudi_tables(base_path:str="hudi_tables"):
    tables = {}
    base = Path(base_path)
    if base.exists():
        for table_dir in base.iterdir():
            if table_dir.is_dir():
                table_name = table_dir.name
                tables[table_name] = os.path.abspath(str(table_dir)).replace("\\", "/")
    return tables


@app.on_event('startup')
def startup():
    global spark, HUDI_TABLES
    spark = SparkSession.builder \
    .appName("Fast API Servers") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.jars.packages", "org.apache.hudi:hudi-spark3.3-bundle_2.12:0.14.1") \
    .getOrCreate()

    HUDI_TABLES = discover_hudi_tables()

@app.get("/query/{table_name}")
def query(table_name: str):
    if table_name not in HUDI_TABLES:
        raise HTTPException(status_code=404, detail="Table not found")
    try:
        path = HUDI_TABLES[table_name]
        # base_path = os.path.abspath("hudi_tables").replace("\\", "/")
        df = spark.read.format("hudi").load(path)
        data = df.toPandas().to_dict(orient='records')
        return {'data': data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)

#
# from fastapi import FastAPI
# import uvicorn
# # Create an instance of FastAPI
# app = FastAPI()
#
# # Define a simple route
# @app.get("/")
# def read_root():
#     return {"message": "Welcome to the Data Server!"}
#
# # Define a route with a path parameter
# @app.get("/items/{item_id}")
# def read_item(item_id: int):
#     return {"item_id": item_id, "message": f"Item {item_id} details here."}
#
# # Define a POST route
# @app.post("/items/")
# def create_item(item: dict):
#     return {"message": "Item created", "item_data": item}
#
# # Define another sample route
# @app.get("/status")
# def status():
#     return {"status": "Data Server is running!"}
#
# if __name__ == '__main__':
#     uvicorn.run(app, host='1.0.0.0', port=8080)