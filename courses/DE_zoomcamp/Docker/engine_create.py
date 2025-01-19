import pandas as pd
from sqlalchemy import create_engine


df = pd.read_csv("yellow_tripdata_2021-01.csv", nrows=100)
# para conseguir los campos datetime en el formato correcto
df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
engine.connect()


