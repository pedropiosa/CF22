from pyspark.sql import Column, DataFrame
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import array
import json


def convert2tuples(arr:array):
    res = []
    for t in arr:
        res.append((t[0],t[1]))

    return res

def read_json(file_name:str):
    # Opening JSON file
    f = open(file_name)

    # returns JSON object as a dictionary
    return json.load(f)

def read_frontfile(spark: SparkSession,
                   path: str,
                   format: str,
                   root_tag: str,
                   row_tag: str):
    df = spark.read.format(format) \
        .options(rootTag=root_tag) \
        .options(rowTag=row_tag) \
        .options(inferSchema=False) \
        .load(path)

    return df


def read_data(spark: SparkSession,
              path: str,
              format: str,
              cols_name: array = [],
              header: bool = True,
              sep: str = "╬",
              lineSep: str = "\n",
              multiline: bool = False):
    df = spark.read \
        .format(format) \
        .option("header", header) \
        .option("sep", sep) \
        .option("lineSep", lineSep) \
        .option("multiLine", multiline) \
        .option("recursiveFileLookup", True) \
        .load(path)

    if len(cols_name) == 0:
        return df
    else:
        return df.toDF(*cols_name)

    return df


def write_data(df: DataFrame,
               path: str,
               format: str,
               mode: str = "overwrite",
               header: bool = True,
               sep: str = "╬",
               multiline: bool = False):
    # today = date.today().strftime("%Y%m%d")
    # current_time = datetime.now().strftime("%H%M%S")
    df.write \
        .format(format) \
        .mode(mode) \
        .option("sep", sep) \
        .option("header", header) \
        .option("multiLine", multiline) \
        .save(f"{path}")


def __split_col(df: DataFrame, col: str, expr: str) -> Column:
    return F.split(df[col], expr)


def jsonify_cols(df):
    return [F.to_json(c[0]).alias(c[0]) if c[1].startswith("struct") or c[1].startswith("array") or c[1].startswith(
        "map") else F.col(c[0])
            for c in df.dtypes]
