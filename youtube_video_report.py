import os
import argparse
import pandas as pd
import boto3
import logging
import datetime
from googleapiclient.discovery import build
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']
API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'

CLIENT_SECRETS_FILE = '/Users/scoyne/Documents/GitHub/lib-global-analytics/Secrets/client_secret.json'
CLIENT_AUTH_TOKEN = '/Users/scoyne/Documents/GitHub/lib-global-analytics/Secrets/youtube_reporting_credentials.json'
COLUMNS = ['audienceWatchRatio', 'relativeRetentionPerformance', 'views']


S3_BUCKET = 'teamanalytics'
TODAY = datetime.datetime.now()
DEFAULT_START_DATE = '2010-01-01'
DEFAULT_END_DATE = TODAY
FILE_NAME = 'youtube_reporting_by_video_{}.csv'.format(TODAY)

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def get_parser():
    # parse arguments
    parser = argparse.ArgumentParser(
        description="Automatically generate youtube analytics report and upload to S3"
    )
    parser.add_argument(
        "--start_date",
        action="store",
        dest="start_date",
        type=valid_date,
        help="The start date of the youtube report, format must be yyyy-mm-dd",
        default=DEFAULT_START_DATE,
    )
    parser.add_argument(
        "--end_date",
        action="store",
        dest="end_date",
        type=valid_date,
        help="The end date of the youtube report, format must be yyyy-mm-dd",
        default=DEFAULT_END_DATE,
    )
    return parser


def upload_to_s3(inputFile, outputFile, bucket):
    logger.info("Start upload to S3 process")
    # Create an S3 client
    s3 = boto3.client('s3')
    # Uploads the given file using a managed uploader, which will split up large
    # files automatically and upload parts in parallel.
    s3.upload_file(inputFile, bucket, outputFile)
    logger.info("End upload to S3 process")


def get_authenticated_service():
    logger.info("Start authentication process")

    credential_path = CLIENT_AUTH_TOKEN
    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRETS_FILE, SCOPES)
        credentials = tools.run_flow(flow, store)

    logger.info("End authentication process")
    return build(API_SERVICE_NAME, API_VERSION, credentials=credentials)


def execute_api_request(client_library_function, **kwargs):
    result = client_library_function(**kwargs).execute()
    return result


def save_report_to_csv(result, columns, filename):
    logger.info("Start save to csv process")
    data = result['rows']
    header = ['video'] + columns
    df = pd.DataFrame(data, columns=header)
    df.to_csv(filename)
    logger.info("End save to csv process")


def main(args):
    startDate = args.start_date.date()
    endDate = args.end_date.date()

    # Disable OAuthlib's HTTPs verification when running locally.
    # *DO NOT* leave this option enabled when running in production.
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

    youtubeAnalytics = get_authenticated_service()

    stringColumns = ",".join(COLUMNS)

    report = execute_api_request(
        youtubeAnalytics.reports().query,
        ids='channel==MINE',
        startDate=startDate,
        endDate=endDate,
        metrics=stringColumns,
        dimensions='video==Nl5ELeRtrcY',
    )

    save_report_to_csv(report, COLUMNS, FILE_NAME)

    outputFile = 'analytics_report/marketing/youtube/{}'.format(FILE_NAME)
    upload_to_s3(FILE_NAME, outputFile, S3_BUCKET)


if __name__ == "__main__":
    main(get_parser().parse_args())
