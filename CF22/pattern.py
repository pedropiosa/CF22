from utils.spark import getSparkSession
from utils.utils import *
from utils.logger import *


class Pattern:

    def __init__(self,
                 text_data_path: str,
                 front_file_path: str,
                 output_path: str,
                 format: str,
                 write_mode: str):
        self.spark = getSparkSession("Patents")
        self.logger = LoggerProvider().get_logger(self.spark)
        self.textdata_file_path = text_data_path
        self.frontfile_file_path = front_file_path
        self.proc_file_path = output_path
        self.format = format
        self.write_mode = write_mode

    def etl(self,
            input_front_path: str,
            input_textdata_path: str,
            input_front_format: str,
            input_textdata_format: str,
            output_path: str,
            output_format: str,
            write_mode: str):

        df_ff = read_data(spark=self.spark,
                          path=input_front_path,
                          format=input_front_format,
                          header=True,
                          sep="╬")

        df_td = read_data(spark=self.spark,
                          path=input_textdata_path,
                          format=input_textdata_format,
                          header=True,
                          sep="╬")

        df_trans = self.transform(df_front_file=df_ff,
                                  df_text_data=df_td)

        write_data(df=df_trans,
                   path=output_path,
                   format=output_format,
                   mode=write_mode)

    @staticmethod
    def transform(df_front_file: DataFrame,
                       df_text_data: DataFrame):
        # filtramos los front files que tienen como publ-document-number el prefijo EP porque
        # los text data son todos EP
        df_ff_filt = df_front_file.filter(F.col("publ-document-number").startswith("EP")) \
            .select("publ-document-number", "cpc")

        # hacer el join con posibilidad que sea BROADCAST join
        df_final = df_text_data.join(df_ff_filt, df_text_data.EPDocNumber == F.col("publ-document-number"), "inner")
        return df_final.select(*df_text_data.columns, df_ff_filt.cpc)