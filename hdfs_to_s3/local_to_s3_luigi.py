import s3fs
from hdfs_to_s3.config_helper import get_aws_credentials
import luigi
import datetime
from hdfs_to_s3 import pipeline_config
from hdfs import InsecureClient
import logging
import uuid
import os
from logging.handlers import RotatingFileHandler
from os import walk
import watchtower
from boto3.session import Session


pid = os.getpid()
(aws_access_key_id,
 aws_secret_access_key,
 aws_session_token,
 session_token_expiry) = get_aws_credentials()

boto3_session = Session(aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        aws_session_token=aws_session_token,
                        region_name='us-west-2'
                        )

log = logging.getLogger(str(pid))
formatter = logging.Formatter(f'{pid} '
                              f'%(asctime)s - '
                              f' - %(levelname)s - %(filename)s - %(lineno)s - %(message)s')

dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, f'logs/local_to_s3_{pid}.log')
fh = RotatingFileHandler(filename, maxBytes=1000000, backupCount=1)
fh.setFormatter(formatter)
log.addHandler(fh)

handler = watchtower.CloudWatchLogHandler(boto3_session=boto3_session,
                                          log_group='shopping_midt_s3',
                                          stream_name=f'shopping_midt_s3_{pid}',
                                          create_log_group=True,
                                          create_log_stream=True)
handler.setFormatter(formatter)
log.addHandler(handler)
log.setLevel(logging.INFO)


def log_timer(func):
    def inner(*args, **kwargs):
        start = datetime.datetime.now()
        log.info('starting: ' + str(start))

        func(*args, **kwargs)

        end = datetime.datetime.now()
        log.info('elapsed time: ' + str(end - start))

    return inner


class TaskTemplate(luigi.Task):
    job_uuid = luigi.Parameter()
    env_config = luigi.DictParameter(visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    hour = luigi.Parameter()
    filename = luigi.Parameter()
    iscomplete = False

    @staticmethod
    def get_s3fs():
        (aws_access_key_id,
         aws_secret_access_key,
         aws_session_token,
         session_token_expiry) = get_aws_credentials()
        return s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token
        )

    def get_hdfs_client(self):
        return InsecureClient(self.env_config['hdfs_server'], user=self.env_config['hdfs_user'])

    def get_token_path(self):
        token_path = (self.env_config['s3_luigi_status_location']
                      + self.get_task_family() + '/'
                      + self.year + '_'
                      + self.month + '_'
                      + self.day + '_'
                      + self.hour + '_'
                      + self.filename)
        return token_path

    def complete(self):
        if not self.iscomplete:
            token_path = self.get_token_path()
            log.info(f'{self.job_uuid} checking to see if task is complete ' + token_path)

            if self.get_s3fs().exists(token_path):
                log.info("it's complete")
                self.iscomplete = True
                return True
            else:
                log.info("not complete")
                return False
        else:
            return True

    def mark_complete(self):
        token_path = self.get_token_path()
        log.info('marking task as complete ' + token_path)
        self.get_s3fs().touch(token_path)
        self.iscomplete = True


class DownloadOneFileFromHDFSResponse(luigi.ExternalTask):
    job_uuid = luigi.Parameter()
    env_config = luigi.DictParameter(visibility=luigi.parameter.ParameterVisibility.HIDDEN)
    year = luigi.Parameter()
    month = luigi.Parameter()
    day = luigi.Parameter()
    hour = luigi.Parameter()
    filename = luigi.Parameter()

    @staticmethod
    def get_s3fs():
        (aws_access_key_id,
         aws_secret_access_key,
         aws_session_token,
         session_token_expiry) = get_aws_credentials()
        return s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            token=aws_session_token
        )

    def get_token_path(self):
        token_path = (self.env_config['s3_luigi_status_location']
                      + self.task_family + '/'
                      + self.year + '_'
                      + self.month + '_'
                      + self.day + '_'
                      + self.hour + '_'
                      + self.filename)
        return token_path

    def complete(self):
        log.info(f'DownloadOneFileFromHDFSResponse.complete: job_uuid {self.job_uuid} pid {os.getpid()}')

        token_path = self.get_token_path()
        log.info('checking to see if task is complete ' + token_path)

        if self.get_s3fs().exists(token_path):
            log.info("it's complete")
            return True
        else:
            log.info("not complete")
            return False


class UploadOneFiletoS3Response(TaskTemplate):
    # resources = {'s3': 1}

    def get_s3_file_path(self):
        s3_file_path = self.env_config['s3_response_file_path'].format(self.year,
                                                                       self.month,
                                                                       self.day,
                                                                       self.hour,
                                                                       self.filename)
        return s3_file_path

    def requires(self):
        log.info(f'UploadOneFiletoS3Response.requires: job_uuid {self.job_uuid} pid {os.getpid()}')

        s3_file_path = self.get_s3_file_path()
        log.info(f'checking for existing file on s3: {s3_file_path}')
        s3 = self.get_s3fs()
        if not s3.exists(s3_file_path):
            return DownloadOneFileFromHDFSResponse(
                job_uuid=self.job_uuid,
                env_config=self.env_config,
                year=self.year,
                month=self.month,
                day=self.day,
                hour=self.hour,
                filename=self.filename
            )
        else:
            log.warning(f'file already exists on s3 - skipping download for {s3_file_path}')
            return None

    @log_timer
    def run(self):
        log.info(f'UploadOneFiletoS3Response.run: job_uuid {self.job_uuid} pid {os.getpid()}')
        local_directory = 'data'
        s3_file_path = self.get_s3_file_path()
        log.info(f'uploading file to s3: {s3_file_path}')
        s3 = self.get_s3fs()
        if not s3.exists(s3_file_path):
            s3.put(os.path.join(local_directory, self.filename), s3_file_path)
        else:
            log.warning(f'file already exists on s3 - skipping upload for {s3_file_path}')
        self.mark_complete()


class DeleteLocalFileResponse(TaskTemplate):

    def requires(self):
        log.info(f'DeleteLocalFileResponse.requires: job_uuid {self.job_uuid} pid {os.getpid()}')
        return UploadOneFiletoS3Response(
            job_uuid=self.job_uuid,
            env_config=self.env_config,
            year=self.year,
            month=self.month,
            day=self.day,
            hour=self.hour,
            filename=self.filename,
        )

    def complete(self):
        return self.iscomplete

    def run(self):
        log.info(f'DeleteLocalFileResponse.run: job_uuid {self.job_uuid} pid {os.getpid()}')
        local_file_names = []
        for (dirpath, dirnames, filenames) in walk('data'):
            local_file_names.extend(filenames)

        for file_name in local_file_names:
            if self.filename in file_name:
                local_file_path = os.path.join('data', self.filename)
                log.info('deleting local file: ' + local_file_path)
                try:
                    os.remove(local_file_path)
                except FileNotFoundError as e:
                    log.warning(e)
        self.iscomplete = True


class Find_All_Local_Files_For_Hour(TaskTemplate):

    def complete(self):
        log.info(f'Find_All_Local_Files_For_Hour.complete: job_uuid {self.job_uuid} pid {os.getpid()}')
        return self.iscomplete

    def requires(self):
        log.info(f'Find_All_Local_Files_For_Hour.requires: job_uuid {self.job_uuid} pid {os.getpid()}')
        log.info(f'looking for local files {self.year} {self.month} {self.day} {self.hour}')

        local_file_names = []
        for (dirpath, dirnames, filenames) in walk('data'):
            local_file_names.extend(filenames)

        task_list = []
        for file_name in local_file_names:
            if file_name != '.gitignore' and '.temp-' not in file_name:
                log.info(f"creating task - {file_name}")
                task_list.append(
                    DeleteLocalFileResponse(
                        job_uuid=self.job_uuid,
                        env_config=self.env_config,
                        year=self.year,
                        month=self.month,
                        day=self.day,
                        hour=self.hour,
                        filename=file_name
                    )
                )

        return task_list

    def run(self):
        self.iscomplete = True


if __name__ == '__main__':
    run_timestamp = str(datetime.datetime.now()).replace(' ', '_').replace(':', '_')
    log.info(run_timestamp)
    job_uuid = hex(uuid.uuid1().int >> 96)[2:]
    log.info(f'creating new job_uuid {job_uuid}')

    luigi.build([
        Find_All_Local_Files_For_Hour(
            env_config=pipeline_config.prod_config,
            year='2020',
            month='02',
            day='01',
            hour='23',
            filename='',
            job_uuid=job_uuid
        )
    ],
        workers=3,
        local_scheduler=False,
        # scheduler_host='127.0.0.1',
        detailed_summary=False
    )
