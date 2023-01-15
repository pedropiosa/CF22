from utils.spark import getSparkSession
from utils.utils import *
from utils.logger import *
import array


class GreenPlastic:

    def __init__(self,
                 input_proc_path: str,
                 input_find_path: str,
                 output_path: str,
                 format: str,
                 write_mode: str):
        self.spark = getSparkSession("Patents")
        self.logger = LoggerProvider().get_logger(self.spark)
        self.input_proc_path = input_proc_path
        self.input_find_path = input_find_path
        self.output_path = output_path
        self.format = format
        self.write_mode = write_mode

    def etl(self,
            input_proc_path: str,
            input_proc_format: str,
            input_find_path: str,
            input_find_format: str,
            input_cols: array,
            output_path: str,
            output_format: str,
            output_cols: array,
            write_mode: str):

        df_proc = read_data(spark=self.spark,
                            path=input_proc_path,
                            format=input_proc_format,
                            cols_name=input_cols,
                            header=True)

        df_find = read_data(spark=self.spark,
                            path=input_find_path,
                            format=input_find_format,
                            cols_name=input_cols,
                            header=True)

        df_green = self.transform(df_proc, df_find, output_cols)

        write_data(df=df_green,
                   path=output_path,
                   format=output_format,
                   mode=write_mode)

    @staticmethod
    def transform(df_proc, df_find, output_cols) -> DataFrame:
        df_join = df_proc.join(df_find,
                               df_proc["EPDocNumber"] == df_find["EPDocNumber"],
                               "inner")

        return df_join.select(*output_cols)