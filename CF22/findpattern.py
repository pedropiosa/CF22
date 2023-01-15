import re

import spacy as spacy
from utils.spark import getSparkSession
from utils.utils import *
from utils.logger import *
import array


class FindPattern:

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
            patterns: array,
            max_d: int,
            output_path: str,
            output_format: str,
            write_mode: str):
        # Load the Spacy language model
        # nlp = spacy.load("en_core_web_sm")

        self.logger.info("Initializing find patterns")
        df = read_data(self.spark,
                       path=input_path,
                       format=input_format,
                       cols_name=input_cols,
                       header=True,
                       sep="╬")

        df_final = self.transform(df, patterns, max_d)

        cols = jsonify_cols(df_final)

        df_final_jsonify = df_final.select(cols)

        write_data(df_final_jsonify,
                   path=output_path,
                   format=output_format,
                   mode=write_mode,
                   header=True,
                   sep="╬")

    @staticmethod
    def transform(df: DataFrame, patterns: array, max_d: int):

        # .filter(F.col("Abstr").isNotNull()) \
        df_proc = df \
            .select(*["EPDocNumber", "Descr", "Abstr"]).rdd.map(
            lambda t: (t[0], t[2], find_words(t[1], patterns, max_d))).toDF(
            ["EPDocNumber", "Abstr", "tuple"])

        # found_count, sentences, pattern_counts, totalSentences, totalWords, occurrencesPer100Words, occurrencesPercentage
        df_final = df_proc.select("EPDocNumber",
                                  "Abstr",
                                  F.col("tuple._1").alias("found_count"),
                                  F.col("tuple._2").alias("sentences"),
                                  F.col("tuple._3").alias("pattern_counts"),
                                  F.col("tuple._4").alias("totalSentences"),
                                  F.col("tuple._5").alias("totalWords"),
                                  F.col("tuple._6").alias("occurrencesPer100Words"),
                                  F.col("tuple._7").alias("patterns_count_amount"))

        return df_final


def find_words(text: str, word_tuples: array, max_distance: int):
    # Initialize a list to keep track of the number of found instances for each text
    sentences = [""]
    pattern_counts = {}
    patterns_count_amount = []

    #  # Loop through the texts
    #  for text in texts:
    # Process the text using the Spacy model
    nlp = spacy.load("en_core_web_sm")
    doc = nlp(text)

    totalSentences = 0
    totalWords = len(doc.text.split())
    numberTuples = len(word_tuples)

    # Initialize a variable to keep track of the number of found instances in the current text
    found_count = 0

    # Loop through the word tuples
    for words in word_tuples:
        # Initialize the dictionary of pattern counts with keys for each pattern and values set to 0
        pattern_counts[words] = 0
        # Define the regular expression patterns for the words, using wildcards
        patterns = [re.compile(word, re.IGNORECASE) for word in words]

        # Loop through the sentences in the text
        for sent in doc.sents:
            # Loop through the tokens (words) in the sentence
            totalSentences += 1

            for i, token in enumerate(sent):
                # Skip punctuation symbols
                if token.is_punct:
                    continue

                # Loop through the regular expression patterns
                for pattern in patterns:
                    # Check if the current token matches the current regular expression pattern

                    if pattern.match(token.text):
                        matched_words = [token.text]
                        matched_patterns = [pattern]

                        # Check if there is a token that matches another regular expression pattern within a distance of max_distance words
                        for j in range(i + 1, min(i + max_distance + 1, len(sent))):
                            for pattern2 in patterns:
                                if pattern2.match(sent[j].text) and pattern2 not in matched_patterns:
                                    matched_patterns.append(pattern2)
                                    matched_words.append((sent[j].text))

                                    # If all the regular expression patterns have been matched, print the sentence and the matched words
                                    if len(matched_patterns) == len(patterns):
                                        sentences.append(sent.text)

                                        # Increment the found_count variable
                                        found_count += 1

                                        # Increment the count for each matched pattern in the pattern_counts dictionary
                                        pattern_counts[words] += 1

        patterns_count_amount.append(pattern_counts[words])

    totalSentences = totalSentences / numberTuples
    occurrencesPer100Words = found_count / (totalWords / 100)

    # Return the list of found counts and the list of tuples that contains the index and the found count for each text
    return found_count, sentences, pattern_counts, totalSentences, totalWords, occurrencesPer100Words, patterns_count_amount
