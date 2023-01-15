from findpattern import FindPattern
from greenplastic import GreenPlastic
from textdata import TextData
from frontfile import FrontFile
from pattern import Pattern
from literals import *
from utils.utils import read_json, convert2tuples
import time, os, sys

if __name__ == "__main__":
    t0 = time.time()
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages com.databricks:spark-xml_2.12:0.15.0 pyspark-shell"
    )
    config_file = read_json(sys.argv[1])

    def launch_etl(config_file):
        # // process_name: [textdata, frontfile, pattern, findpattern, greenplastic]
        if config_file.get("process_name") == "textdata":
            textdata = TextData(input_path=config_file.get("input_path"),
                                output_path=config_file.get("output_path"),
                                format=config_file.get("output_format"),
                                write_mode=config_file.get("write_mode"))

            textdata.etl(input_path=config_file.get("input_path"),
                         input_format=config_file.get("input_format"),
                         input_cols=config_file.get("input_cols"),
                         output_path=config_file.get("output_path"),
                         output_format=config_file.get("output_format"),
                         write_mode=config_file.get("write_mode"))

        elif config_file.get("process_name") == "frontfile":
            frontfile = FrontFile(input_path=f"resources/frontfile/xml/xml22_12_2k",
                                  output_path=f"output/frontfile/xml/xml22_12_2k",
                                  format=format,
                                  write_mode=write_mode)

            frontfile.etl(input_path=config_file.get("input_path"),
                          input_format=config_file.get("input_format"),
                          input_root_tag=config_file.get("input_root_tag"),
                          input_row_tag=config_file.get("input_row_tag"),
                          output_path=config_file.get("output_path"),
                          output_format=config_file.get("output_format"),
                          write_mode=config_file.get("write_mode"))

        elif config_file.get("process_name") == "pattern":
            pattern = Pattern(text_data_path=config_file.get("text_data_path"),
                              front_file_path=config_file.get("front_file_path"),
                              output_path=config_file.get("output_path"),
                              format=config_file.get("format"),
                              write_mode=config_file.get("write_mode"))

            pattern.etl(input_front_path=config_file.get("input_front_path"),
                        input_textdata_path=config_file.get("input_textdata_path"),
                        input_front_format=config_file.get("input_front_format"),
                        input_textdata_format=config_file.get("input_textdata_format"),
                        output_path=config_file.get("output_path"),
                        output_format=config_file.get("output_format"),
                        write_mode=config_file.get("write_mode"))

        elif config_file.get("process_name") == "findpatterns":
            patt = convert2tuples(config_file.get("patterns"))

            find_patterns = FindPattern(input_path=config_file.get("input_front_path"),
                                        output_path=config_file.get("output_path"),
                                        format=config_file.get("format"),
                                        write_mode=config_file.get("write_mode"), )

            find_patterns.etl(input_path=config_file.get("input_path"),
                              input_format=config_file.get("input_format"),
                              input_cols=config_file.get("input_cols"),
                              patterns=patt,
                              max_d=config_file.get("max_d"),
                              output_path=config_file.get("output_path"),
                              output_format=config_file.get("output_format"),
                              write_mode=config_file.get("write_mode"))

        elif config_file.get("process_name") == "greenplastics":

            green = GreenPlastic(input_proc_path=config_file.get("input_proc_path"),
                                 input_find_path=config_file.get("input_find_path"),
                                 output_path=config_file.get("output_path"),
                                 format=config_file.get("format"),
                                 write_mode=config_file.get("write_mode"))

            green.etl(input_proc_path=config_file.get("input_proc_path"),
                      input_proc_format=config_file.get("input_proc_format"),
                      input_find_path=config_file.get("input_find_path"),
                      input_find_format=config_file.get("input_find_format"),
                      input_cols=config_file.get("input_cols"),
                      output_path=config_file.get("output_path"),
                      output_format=config_file.get("output_format"),
                      output_cols=config_file.get("output_cols"),
                      write_mode=config_file.get("write_mode"))


    launch_etl(config_file)

    t1 = time.time()

    total = t1 - t0
    print(str(total))
