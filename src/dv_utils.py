from pyspark.sql.functions import sha2, concat_ws, lit, current_timestamp

def generate_hash(df, cols, new_col_name):
    """
    Adds a SHA-256 hash column based on given columns.
    """
    return df.withColumn(new_col_name, sha2(concat_ws("||", *cols), 256))

def add_audit_columns(df):
    """
    Adds Load_Date and Record_Source columns.
    """
    return df.withColumn("Load_Date", current_timestamp()) \
             .withColumn("Record_Source", lit("raw_ingest"))
