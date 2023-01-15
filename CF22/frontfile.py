from utils.spark import getSparkSession
from utils.utils import *
from utils.logger import *
import struct


class FrontFile:

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
            input_root_tag: str,
            input_row_tag: str,
            output_path: str,
            output_format: str,
            write_mode: str):

        df_raw = read_frontfile(self.spark,
                                path=input_path,
                                format=input_format,
                                root_tag=input_root_tag,
                                row_tag=input_row_tag)

        df_trans = self.transform(df_raw)

        write_data(df=df_trans,
                   path=output_path,
                   format=output_format,
                   mode=write_mode,
                   header=True, sep="╬")

    @staticmethod
    def transform(df):
        df_parent = df.filter(F.col("_country") == "EP") \
            .select(
            "_country",
            "_doc-number",
            "_family-id",
            "_kind",
            "_system",
            F.col("bibliographic-data.publication-reference.document-id").alias("publ-document-id"),
            F.col("bibliographic-data.patent-classifications.patent-classification"),
            F.col("bibliographic-data.patent-classifications.combination-set").alias(
                "patent-classifications-combination-set"),
            F.col("bibliographic-data.invention-title"),
            F.col("abstract._lang").alias("abstract-lang"),
            F.col("abstract.p").alias("abstract"))

        # Converting function to UDF
        convertUDF = F.udf(lambda array_cpc: udf_parse_cpc(array_cpc), F.StringType())

        df_expl = df_parent \
            .withColumn("publ-document-id-expl", F.explode(F.col("publ-document-id"))) \
            .withColumn("invention-title-expl", F.explode(F.col("invention-title"))) \
            .select("*", convertUDF(F.col("patent-classification")).alias("cpc"))

        df_expl_childs = df_expl.select(
            "_country",
            "_doc-number",
            "_family-id",
            "_kind",
            "_system",
            F.col("publ-document-id-expl._document-id-type").alias("publ-attr-document-type"),
            F.col("publ-document-id-expl.country").alias("publ-document-country"),
            F.col("publ-document-id-expl.doc-number").alias("publ-document-number"),
            F.col("publ-document-id-expl.kind").alias("publ-document-kind"),
            F.col("publ-document-id-expl.date").alias("publ-document-date"),
            "cpc",
            F.col("invention-title-expl._VALUE").alias("invention-title"),
            F.col("invention-title-expl._lang").alias("invention-title-lang"),
            "abstract-lang",
            "abstract") \
            .filter(F.col("invention-title-lang") == "en") \
            .filter(F.col("publ-attr-document-type") == "epodoc")

        cols = jsonify_cols(df_expl_childs)

        return df_expl_childs.select(cols)


def udf_parse_cpc(patent_classification: struct):
    array_cpc = []
    for patent in patent_classification:
        cpc = f"{patent.section}{getattr(patent, 'class')}{patent.subclass}{getattr(patent, 'main-group')}/{patent.subgroup}"
        array_cpc.append(cpc)
    return array_cpc
