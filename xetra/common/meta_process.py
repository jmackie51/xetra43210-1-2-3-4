"""
Methods for processing the meta file
"""
import collections
from datetime import datetime, timedelta

import pandas as pd

from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException

class MetaProcess():
    """
    class for working with the meta file
    """

    @staticmethod
    def update_meta_file(bucket: S3BucketConnector, meta_key: str, extract_date_list: list):
        """
        Updating the meta file with the processed Xetra dates and todays date as processed date

        :param: extract_date_list -> a list of dates that are extracted from the source
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file
        """
        # Cerating an empty DataFrame using the feta file column names
        df_new = pd.DataFrame(columns=[MetaProcessFormat.META_SOURCE_DATE_COL,
                                       MetaProcessFormat.META_PROCESS_COL])
        # Fill the date column wwith extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL] = extract_date_list
        #Fill the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL] = datetime.today().strftime(
            MetaProcessFormat.META_PROCESS_DATE_FORMAT)
        try:
             # If meta file exists -> union DataFrame of old and new meta data is created
            df_old = bucket.read_csv_to_df(bucket, meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
            bucket.write_df_to_s3_csv(bucket, df_all, meta_key)
        except bucket.session.client('s3').exceptions.NoSuchKey:
            # No meta file exists -> only the new data is used
            df_all = df_new
        #write to s3
        bucket.write_df_to_s3(meta_key, df_all, MetaProcessFormat.META_FILE_FORMAT.value)
        return True


    @staticmethod
    def return_date_list(bucket: S3BucketConnector,
                         start_date: str, meta_key: str, today_date: str = '2022-12-31'):
        """
        Creating a list of dates based on the input first_date and the already
        processed dates in the meta file

        :param: first_date -> the earliest date Xetra data should be processed
        :param: meta_key -> key of the meta file on the S3 bucket
        :param: s3_bucket_meta -> S3BucketConnector for the bucket with the meta file

        returns:
          min_date: first date that should be processed
          return_date_list: list of all dates from min_date till today
        """
        #def return_date_list(bucket, arg_date, src_format, meta_key, today_date):
        min_date = datetime.strptime(start_date, MetaProcessFormat.META_DATE_FORMAT.value).date() \
             - timedelta(days=1)
        today = datetime.strptime(today_date, MetaProcessFormat.META_DATE_FORMAT.value).date()
        try:
            # If meta file exists create return_date_list using the content of the meta file
            # Reading meta file
            df_meta = bucket.read_csv_to_df(bucket, meta_key)
            # Creating a list of dates from first_date untill today
            dates = [(min_date + timedelta(days=x)) for x in range(0, (today-min_date).days + 1)]
            # Creating set of all dates in meta file
            src_dates = set(pd.to_datetime(df_meta[MetaProcessFormat.META_SOURCE_DATE_COL]).dt.date)
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                # Determining the earliest date that should be extracted
                min_date = min(set(dates[1:]) - src_dates) - timedelta(days=1)
                # Creating a list of dates from min_date untill today
                return_dates = [date.strftime(MetaProcessFormat.META_DATE_FORMAT.value)
                                for date in dates if date >= min_date]
                return_min_date = (min_date + timedelta(days=1)) \
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
            else:
                return_dates = []
                return_min_date = datetime(2200, 1, 1).date()\
                    .strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        except bucket.meta.client.exceptions.NoSuchKey:
            # No meta file found -> creating a date list from first_date - 1 day untill today
            return_dates = [(min_date + timedelta(days=x)) \
                            .strftime(MetaProcessFormat.META_DATE_FORMAT.value) \
                                for x in range(0, (today-min_date).days + 1)]
            return_min_date = start_date
        return return_min_date, return_dates
