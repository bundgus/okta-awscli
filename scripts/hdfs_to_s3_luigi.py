import s3fs
import scripts.pyathena_config as pac
import luigi
import datetime
from scripts import pipeline_config
from hdfs import InsecureClient
import logging
import uuid
import os
from logging.handlers import RotatingFileHandler

log = logging.getLogger("luigi-interface")
job_uuid = hex(uuid.uuid1().int >> 96)[2:]
formatter = logging.Formatter('<LOGMSGSTART>' +
                              '%(asctime)s - ' + job_uuid +
                              ' - %(levelname)s - %(filename)s - %(lineno)s - %(message)s')
dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, 'logs/process_pipeline.log')
fh = RotatingFileHandler(filename, maxBytes=1000000, backupCount=10)
fh.setFormatter(formatter)
log.addHandler(fh)


class TaskTemplate(luigi.Task):
    env_config = luigi.DictParameter(visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    subscriber_code = luigi.Parameter()
    extract_year = luigi.Parameter()
    extract_month = luigi.Parameter()
    extract_day = luigi.Parameter()
    file_name = luigi.Parameter()
    iscomplete = False

    def complete(self):
        if not self.iscomplete:
            (aws_access_key_id,
             aws_secret_access_key,
             aws_session_token,
             session_token_expiry) = pac.get_aws_credentials()

            fs = s3fs.S3FileSystem(
                key=aws_access_key_id,
                secret=aws_secret_access_key,
                token=aws_session_token
            )
            token_path = (self.env_config['s3_luigi_status_location']
                          + self.task_family + '/'
                          + self.subscriber_code + '_'
                          + self.extract_year + '_'
                          + self.extract_month + '_'
                          + self.extract_day + '_'
                          + self.file_name)
            log.info('checking to see if task is complete ' + token_path)

            if fs.exists(token_path):
                log.info("it's complete")
                self.iscomplete = True
                return True
            else:
                log.info("not complete")
                return False
        else:
            return True

    def mark_complete(self):
        aws_access_key_id, aws_secret_access_key, aws_session_token, session_token_expiry = pac.get_aws_credentials()
        fs = s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token
        )

        # task_hash = hex(hash128(str(self.to_str_params(only_significant=True, only_public=True))))
        token_path = (self.env_config['s3_luigi_status_location']
                      + self.task_family + '/'
                      + self.subscriber_code + '_'
                      + self.extract_year + '_'
                      + self.extract_month + '_'
                      + self.extract_day + '_'
                      + self.file_name)
        log.info('marking task as complete ' + token_path)
        fs.touch(token_path)
        self.iscomplete = True


class HDFSFileReady(luigi.ExternalTask):
    env_config = luigi.DictParameter()
    file_name: str = luigi.Parameter()
    iscomplete = False

    def complete(self):
        if not self.iscomplete:
            log.info('checking to see if file is on hdfs ' + self.file_name)
            client = InsecureClient(self.env_config['hdfs_server'], user=self.env_config['hdfs_user'])
            hdfs_directory = self.env_config['hdfs_extract_directory']
            status = client.status(hdfs_directory + self.file_name, strict=False)
            if status is not None:
                log.info('hdfs found status: ' + str(status))
                log.info('found hdfs file: ' + self.file_name)
                self.iscomplete = True
                return True
            else:
                log.info('hdfs not found status: ' + str(status))
                log.info('hdfs file not found: ' + self.file_name)
                return False
        else:
            return True


class DownloadOneFileFromHDFS(TaskTemplate):

    def requires(self):
        return HDFSFileReady(env_config=self.env_config,
                             file_name=self.file_name
                             )

    def run(self):
        local_directory = 'data'
        log.info('downloading file: ' + self.file_name)
        start = datetime.datetime.now()
        log.info('starting: ' + str(start))
        client = InsecureClient(self.env_config['hdfs_server'], user=self.env_config['hdfs_user'])
        hdfs_directory = self.env_config['hdfs_extract_directory']
        client.download(hdfs_directory + self.file_name, local_directory, overwrite=True)

        end = datetime.datetime.now()
        log.info('download complete: ' + str(end))
        log.info('elapsed time: ' + str(end - start))

        self.mark_complete()


class UploadOneFiletoS3(TaskTemplate):

    def requires(self):
        return DownloadOneFileFromHDFS(env_config=self.env_config,
                                       file_name=self.file_name,
                                       subscriber_code=self.subscriber_code,
                                       extract_year=self.extract_year,
                                       extract_month=self.extract_month,
                                       extract_day=self.extract_day
                                       )

    def run(self):
        log.info('uploading file: ' + self.file_name)
        start = datetime.datetime.now()
        log.info('starting: ' + str(start))

        aws_access_key_id, aws_secret_access_key, aws_session_token, session_token_expiry = pac.get_aws_credentials()
        fs = s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token
        )
        fs.put(os.path.join('data', self.file_name), self.env_config['s3_extract_landing_location'] + self.file_name)
        end = datetime.datetime.now()
        log.info('upload complete: ' + str(end))
        log.info('elapsed time: ' + str(end - start))

        self.mark_complete()


class DeleteLocalFile(TaskTemplate):

    def requires(self):
        return UploadOneFiletoS3(env_config=self.env_config,
                                 file_name=self.file_name,
                                 subscriber_code=self.subscriber_code,
                                 extract_year=self.extract_year,
                                 extract_month=self.extract_month,
                                 extract_day=self.extract_day
                                 )

    def run(self):
        log.info('deleting local file: ' + self.file_name)
        start = datetime.datetime.now()
        log.info('starting: ' + str(start))
        try:
            os.remove(os.path.join('data', self.file_name))
        except FileNotFoundError as e:
            log.info(e)
        end = datetime.datetime.now()
        log.info('delete complete: ' + str(end))
        log.info('elapsed time: ' + str(end - start))

        self.mark_complete()


class UploadOneSubscriberDailyExtractToS3(luigi.Task):
    date_string = luigi.Parameter(visibility=luigi.parameter.ParameterVisibility.PRIVATE)
    env_config = luigi.DictParameter(visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    subscriber_code = luigi.Parameter()
    extract_year = luigi.Parameter()
    extract_month = luigi.Parameter()
    extract_day = luigi.Parameter()
    iscomplete = False

    def complete(self):
        return self.iscomplete

    def requires(self):
        filename_summary = self.env_config['filename_summary'].format(
            self.env_config['env_name'],
            self.env_config['version'],
            self.date_string,
            self.subscriber_code,
        )
        filename_request = self.env_config['filename_request'].format(
            self.env_config['env_name'],
            self.env_config['version'],
            self.date_string,
            self.subscriber_code,
        )
        filename_response = self.env_config['filename_response'].format(
            self.env_config['env_name'],
            self.env_config['version'],
            self.date_string,
            self.subscriber_code,
        )

        log.info('derived file names')
        log.info(filename_summary)
        log.info(filename_request)
        log.info(filename_response)

        upstream_tasks = [
            DeleteLocalFile(env_config=self.env_config,
                            file_name=filename_summary,
                            subscriber_code=self.subscriber_code,
                            extract_year=self.extract_year,
                            extract_month=self.extract_month,
                            extract_day=self.extract_day,
                            ),
            DeleteLocalFile(env_config=self.env_config,
                            file_name=filename_request,
                            subscriber_code=self.subscriber_code,
                            extract_year=self.extract_year,
                            extract_month=self.extract_month,
                            extract_day=self.extract_day,
                            ),
            DeleteLocalFile(env_config=self.env_config,
                            file_name=filename_response,
                            subscriber_code=self.subscriber_code,
                            extract_year=self.extract_year,
                            extract_month=self.extract_month,
                            extract_day=self.extract_day,
                            )
        ]
        return upstream_tasks

    def run(self):
        self.iscomplete = True


class ProcessAll(luigi.Task):
    env_config = luigi.DictParameter()
    iscomplete = False
    skipslowtests = luigi.BoolParameter(significant=False, default=False)
    numdays = luigi.IntParameter()
    end_days_offset = luigi.IntParameter()
    subscriber_codes = luigi.ListParameter()

    def complete(self):
        return self.iscomplete

    def requires(self):
        subscriber_codes = self.subscriber_codes
        numdays = self.numdays
        end_days_offset = self.end_days_offset
        now = datetime.datetime.today().date()
        date_string_list = [str(now - datetime.timedelta(days=x + end_days_offset)) for x in range(numdays)]
        date_year_list = [str((now - datetime.timedelta(days=x + end_days_offset)).year) for x in range(numdays)]
        date_month_list = [str((now - datetime.timedelta(days=x + end_days_offset)).month).rjust(2, '0') for x in
                           range(numdays)]
        date_day_list = [str((now - datetime.timedelta(days=x + end_days_offset)).day).rjust(2, '0') for x in
                         range(numdays)]
        date_vals = list(zip(date_string_list, date_year_list, date_month_list, date_day_list))

        upstream_tasks = []

        for subscriber_code in subscriber_codes:
            for date_val in date_vals:
                log.info('{0} {1}'.format(subscriber_code,
                                          date_val[0],
                                          ))
                upstream_tasks.append(UploadOneSubscriberDailyExtractToS3(subscriber_code=subscriber_code,
                                                                          date_string=date_val[0],
                                                                          extract_year=date_val[1],
                                                                          extract_month=date_val[2],
                                                                          extract_day=date_val[3],
                                                                          env_config=self.env_config
                                                                          ))
        return upstream_tasks

    def run(self):
        self.iscomplete = True


if __name__ == '__main__':
    while True:
        run_timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '_')
        print(run_timestamp)
        skipslowtests = True
        luigi.build([
            ProcessAll(skipslowtests=skipslowtests,
                       env_config=pipeline_config.cert_config_v1_4,
                       numdays=5,
                       end_days_offset=1,
                       subscriber_codes=pipeline_config.cert_config_v1_4['subscriber_codes']
                       ),
        ],
            workers=1,
            local_scheduler=True,
            detailed_summary=False
        )
        luigi.build([
            ProcessAll(skipslowtests=skipslowtests,
                       env_config=pipeline_config.prod_config_v1_4,
                       numdays=5,
                       end_days_offset=2,
                       subscriber_codes=pipeline_config.prod_config_v1_4['subscriber_codes'])
        ],
            workers=1,
            local_scheduler=True,
            detailed_summary=False
        )
        break
        # run_timestamp = str(datetime.now()).replace(' ', '_').replace(':', '_')
        # print(run_timestamp)
        # print('sleeping for 1 hour...')
        # sleep(60 * 60)  # sleep for 1 hour
