import os
import sys
import json
from itertools import chain

from pyspark import Row
from pyspark.ml.feature import StringIndexer
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, create_map, lit

def read_csv(file_path, **kwargs):
    print('Reading CSV.')
    return sql_context.read.csv(file_path, **kwargs)

def inspect_data(data):
    # Summarize Missing Values
    missing_values_summary = [(column, data.filter(data[column].isNull()).count(), ) for column in data.columns]
    print(missing_values_summary)
    
    # Examine Distint Values
    for column in data.columns:
        print(column)
        print(data.select(column).orderBy(column).distinct().collect())
        print('\n')

def transform(data, num_partitions=3):
    print('Cleaning Data')
    
    # Columns that require indexing
    categorical_columns = [
        'self_employed',
        'family_history',
        'treatment',
        'no_employees',
        'remote_work',
        'tech_company',
        'benefits',
        'care_options',
        'wellness_program',
        'seek_help',
        'anonymity',
        'leave',
        'mental_health_consequence',
        'phys_health_consequence',
        'coworkers',
        'supervisor',
        'mental_health_interview',
        'phys_health_interview',
        'mental_vs_physical',
        'obs_consequence',
    ]
    
    # Placeholder to store index mappings
    categorical_mappings = {}

    # Replace all NA Strings with None
    data = data.replace(float('nan'), None)
    data = data.replace('N/A', None)
    data = data.replace('NA', None)

    # Drop Columns missing too much Data
    data = data.drop('work_interfere')

    # Child (0-12 years), Adolescence (13-18 years), Adult (19-59 years) and Senior Adult (60 years and above)
    data = data.withColumn('AgeGroup',
                    F.when(data['Age'].between(0, 12), 'Child').\
                    when(data['Age'].between(13, 18), 'Adolescence').\
                    when(data['Age'].between(19, 59), 'Adult').\
                    when(data['Age'].between(60, 120), 'Senior').\
                    otherwise(None)
                  )
    data = data.filter(~data['AgeGroup'].isNull())
    
    # Repartition upon Filter
    data = data.repartition(num_partitions)

    # Conditional Statement: starts with M or m = Male and starts with F or f = Female, otherwise None
    data = data.withColumn('StandardizedGender', 
                F.when(data['Gender'].startswith('M'), 'Male').\
                when(data['Gender'].startswith('m'), 'Male').\
                when(data['Gender'].startswith('F'), 'Female').\
                when(data['Gender'].startswith('F'), 'Female').\
                when(F.lower(data['Gender']).like('%female%'), 'Female').\
                when(F.lower(data['Gender']).like('%male%'), 'Male').\
                when(F.lower(data['Gender']).like('%wom%'), 'Female').\
                when(F.lower(data['Gender']).like('%man%'), 'Male').\
                otherwise(None)
              )

    # Clean Country Values using Replace Dict
    country_replace_dict = {
        'Bahamas, The': 'Bahamas'
    }
    for search_key, replace_value in country_replace_dict.items():
        data = data.replace(search_key, replace_value, subset='Country')


    # Convert to Ordinal Scale
    for column in categorical_columns:
        # fit indexer
        indexer = StringIndexer(inputCol=column, 
                                outputCol='%s_index' % column,
                                handleInvalid='keep').fit(data)
        # convert to index
        data = indexer.transform(data)

        # convert column to integer type
        data = data.withColumn(column, data[column].cast(IntegerType()))

        # save mapping
        categorical_mappings[column] = {idx: label for idx, label in enumerate(indexer.labels)}
    
    # Filter for Columns
    relevant_columns = ['AgeGroup', 'StandardizedGender'] + [column for column in data.columns if '_index' in column]
    data = data.select(relevant_columns)
    
    return data, categorical_mappings

# Download Aggregate View
def download_summary(data, output_directory, num_partitions=1, header=True):    
    """write.csv(path, mode=None, compression=None, sep=None, quote=None, escape=None, 
    header=None, nullValue=None, escapeQuotes=None, quoteAll=None, dateFormat=None, 
    timestampFormat=None)"""
    print('Downloading File.')

    # Create Temp View
    data.select('AgeGroup', 'StandardizedGender', 'Treatment_index').\
        createOrReplaceTempView('mentalHealth')

    # Find Distribution of Mental Health
    query = """
    SELECT StandardizedGender, AgeGroup, Treatment_index, COUNT(*)
        FROM mentalHealth
        GROUP BY StandardizedGender, AgeGroup, Treatment_index
        ORDER BY StandardizedGender, AgeGroup, Treatment_index
    """
    aggregate_treatment = sql_context.sql(query)

    # Delete Temp View
    sql_context.dropTempTable('mentalHealth')  # calls spark.catalog.dropTempView(name)
    
    # aggregate_treatment.write.csv(path=file_path)  # will create multiple files depending on partitions

    # Repartition
    if num_partitions == 1:
        aggregate_treatment.coalesce(num_partitions).write.csv(output_directory, header=header)  # does not overwrite existing file directory
    else:
        aggregate_treatment.repartition(num_partitions).write.csv(output_directory, header=header)
