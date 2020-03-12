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
filename = os.path.join(dirname, f'logs/hdfs_to_local_{pid}.log')
fh = RotatingFileHandler(filename, maxBytes=1000000, backupCount=1)
fh.setFormatter(formatter)
log.addHandler(fh)

handler = watchtower.CloudWatchLogHandler(boto3_session=boto3_session,
                                          log_group='shopping_midt_s3',
                                          stream_name=f'shopping_midt_hdfs_{pid}',
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


class MaxLocalFiles(luigi.ExternalTask):
    def complete(self):
        max_local_files = 10
        f = []
        for (dirpath, dirnames, filenames) in walk('data'):
            f.extend(filenames)

        if len(f) >= max_local_files:
            log.info(f'{max_local_files} or more downloaded files found')
            return False
        else:
            log.info(f'less than {max_local_files} downloaded files found')
            return True


class DownloadOneFileFromHDFSResponse(TaskTemplate):
    # resources = {'hdfs': 1}

    # def requires(self):
    #     return MaxLocalFiles()

    @log_timer
    def run(self):
        log.info(f'DownloadOneFileFromHDFSResponse.run: job_uuid {self.job_uuid} pid {os.getpid()}')
        hdfs_file_path = self.env_config['hdfs_response_file_path'].format(self.year,
                                                                           self.month,
                                                                           self.day,
                                                                           self.hour,
                                                                           self.filename)

        local_directory = 'data'
        log.info(f'{self.job_uuid} pid {os.getpid()} downloading {hdfs_file_path} to {local_directory}')
        self.get_hdfs_client().download(hdfs_file_path, local_directory, overwrite=True)

        self.mark_complete()


class Find_All_Files_For_Hour(TaskTemplate):

    def complete(self):
        return self.iscomplete

    def requires(self):
        log.info(f'looking for files on HDFS {self.year} {self.month} {self.day} {self.hour}')
        client = self.get_hdfs_client()
        hdfs_directory = self.env_config['hdfs_response_file_path'].format(self.year,
                                                                           self.month,
                                                                           self.day,
                                                                           self.hour,
                                                                           '')
        hdfs_file_list = client.list(hdfs_directory, status=True)

        log.info(f'looking for files on S3 {self.year} {self.month} {self.day} {self.hour}')
        s3 = self.get_s3fs()
        s3_directory = self.env_config['s3_response_file_path'].format(self.year,
                                                                       self.month,
                                                                       self.day,
                                                                       self.hour,
                                                                       '')
        s3_file_list = s3.listdir(s3_directory)

        s3_file_sizes = {}
        for s3_file in s3_file_list:
            s3_file_sizes[s3_file['Key'].split('/')[-1]] = s3_file['Size']

        task_list = []
        for file in hdfs_file_list:
            if file[0] in s3_file_sizes and file[1]['length'] == s3_file_sizes[file[0]]:
                log.info(f"skipping task creation - found existing file on S3 {file[0]} {file[1]['length']}")
            else:
                log.info(f"creating task - {file[0]} {file[1]['length']}")
                task_list.append(
                    DownloadOneFileFromHDFSResponse(
                        job_uuid=self.job_uuid,
                        env_config=self.env_config,
                        year=self.year,
                        month=self.month,
                        day=self.day,
                        hour=self.hour,
                        filename=file[0]
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
        Find_All_Files_For_Hour(
            job_uuid=job_uuid,
            env_config=pipeline_config.prod_config,
            year='2020',
            month='02',
            day='01',
            hour='23',
            filename=''
        )
    ],
        workers=2,
        local_scheduler=False,
        detailed_summary=False
    )
