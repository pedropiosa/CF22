from utils.spark import getSparkSession
from utils.utils import *
from utils.logger import *
import array


class TextData:

    def __init__(self,
                 input_path: str,
                 output_path: str,
                 format: str,
                 write_mode: str):
        self.spark = getSparkSession("Patents")
        self.logger = LoggerProvider().get_logger(self.spark)
        self.input_path = input_path
        self.output_path = output_path
        self.format = format
        self.write_mode = write_mode

    def etl(self,
            input_path: str,
            input_format: str,
            input_cols: array,
            output_path: str,
            output_format: str,
            write_mode: str):

        df_raw = read_data(spark=self.spark,
                           path=input_path,
                           format=input_format,
                           cols_name=input_cols,
                           header=False,
                           sep="\t")

        df_trans = self.transfrom(df_raw)

        write_data(df=df_trans,
                   path=output_path,
                   format=output_format,
                   mode=write_mode,
                   header=True,
                   sep="╬")

    @staticmethod
    def transfrom(df):
        df = df.select("Entity",
                       "EPNumber",
                       "Date",
                       "Lang",
                       "Section",
                       "Detail") \
            .filter(df.Lang == "en") \
            .distinct()

        df_title = df.filter(df.Section == "TITLE").select(df.EPNumber.alias("EPNumber"),
                                                           df.Detail.alias("Title"),
                                                           "Date",
                                                           "Entity",
                                                           "Lang").alias("df_title")
        df_abstr = df.filter(df.Section == "ABSTR").select("EPNumber", "Section", df.Detail.alias("Abstr")).alias(
            "df_abstr")
        df_descr = df.filter(df.Section == "DESCR").select("EPNumber", "Section", df.Detail.alias("Descr")).alias(
            "df_descr")

        df_join = df_descr.join(df_abstr, F.col("df_descr.EPNumber") == F.col("df_abstr.EPNumber"), "full") \
            .join(df_title, F.col("df_title.EPNumber") == F.col("df_descr.EPNumber"), "inner") \
            .withColumn("Abstr", F.regexp_replace("df_abstr.Abstr", "<.*?>", "")) \
            .withColumn("Descr", F.regexp_replace("df_descr.Descr", "<.*?>", ""))

        df_epnumber = df_join.withColumn("EPDocNumber", F.concat(F.col("Entity"), F.col("df_title.EPNumber"))) \
            .select("EPDocNumber",
                    "Date",
                    "Title",
                    "Abstr",
                    "Descr",
                    "Entity",
                    F.col("df_title.EPNumber").alias("IDNumber"),
                    "Lang") \
            .drop_duplicates(["EPDocNumber"])

        return df_epnumber
