import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import logging
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set param values
region= "us-east-2"
#S3 bucket path
source_file_path= "s3://data-ingestion-pipeline-demo6157/raw/Test.csv"
target_file_path= "s3://data-ingestion-pipeline-demo6157/curated-data/"
# Crawler name as in CloudFormation template
glueCrawler_name= "data-crawler"

def process_csv_files(source_file_path: str, target_file_path: str):
    """Process CSV data files and convert to parquet format

  
    """

    # Script generated for node S3 bucket
    S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"', 
            "withHeader": True, 
            "separator": ",",
            "optimizePerformance": False,
            },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [
                source_file_path
            ],
            "recurse": True,
        },
        transformation_ctx="S3bucket_node1",
    )

    # Script generated for node S3 bucket
    S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
        frame=S3bucket_node1,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": target_file_path,
            "partitionKeys": [],
        },
        format_options={"compression": "gzip"},
        transformation_ctx="S3bucket_node3",
    )

def start_glue_crawler(glueCrawler_name, region: str):
    """Crawl and catalog the data files

    Args:
        glueCrawler_name (_type_): Glue Crawler name
        region (str): AWS region name

    Returns:
        _type_: Response formatted as JSON
        
    Raises:
        Raise exception if crawler fails to run
    """
    glueClient = boto3.client('glue', region_name=region)
    try:
        results = glueClient.start_crawler(Name= glueCrawler_name)
        return results
    except Exception as startCrawlerException:
        logging.error("Error occurred while starting the Glue Crawler:{}".format(glueCrawler_name))
        raise(startCrawlerException)

def main():
  
    logging.info("Data Pipeline: STARTED")
    
    # 1- Ingest CSV data file(s) to process
    logging.info("Glue ETL Process: STARTED")
    process_csv_files(source_file_path=source_file_path, target_file_path=target_file_path)
    # 2- Start AWS Glue Crawler to catalog the data
    logging.info("Crawler: STARTED")
    start_glue_crawler(glueCrawler_name=glueCrawler_name, region=region)

    logging.info("Data Pipeline: FINISHED")
    job.commit()

if __name__ == "__main__":
    main()
