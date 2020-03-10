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

log = logging.getLogger("luigi-interface")
job_uuid = hex(uuid.uuid1().int >> 96)[2:]
formatter = logging.Formatter('<LOGMSGSTART>' +
                              '%(asctime)s - ' + job_uuid +
                              ' - %(levelname)s - %(filename)s - %(lineno)s - %(message)s')
pid = os.getpid()
dirname = os.path.dirname(__file__)
filename = os.path.join(dirname, f'logs/{pid}_process_pipeline.log')
fh = RotatingFileHandler(filename, maxBytes=1000000, backupCount=10)
fh.setFormatter(formatter)
log.addHandler(fh)


def log_timer(func):
    def inner(*args, **kwargs):
        start = datetime.datetime.now()
        log.info('starting: ' + str(start))

        func(*args, **kwargs)

        end = datetime.datetime.now()
        log.info('elapsed time: ' + str(end - start))

    return inner


class TaskTemplate(luigi.Task):
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
                      + self.task_family + '/'
                      + self.year + '_'
                      + self.month + '_'
                      + self.day + '_'
                      + self.hour + '_'
                      + self.filename)
        return token_path

    def complete(self):
        if not self.iscomplete:
            token_path = self.get_token_path()
            log.info('checking to see if task is complete ' + token_path)

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


class DownloadOneFileFromHDFSRequest(TaskTemplate):
    resources = {'hdfs': 1}

    # def requires(self):
    #     return MaxLocalFiles()

    @log_timer
    def run(self):
        hdfs_file_path = self.env_config['hdfs_response_file_path'].format(self.year,
                                                                           self.month,
                                                                           self.day,
                                                                           self.hour,
                                                                           self.filename)

        local_directory = 'data'
        self.get_hdfs_client().download(hdfs_file_path, local_directory, overwrite=True)

        self.mark_complete()


class UploadOneFiletoS3Request(TaskTemplate):
    resources = {'s3': 1}

    def get_s3_file_path(self):
        s3_file_path = self.env_config['s3_response_file_path'].format(self.year,
                                                                       self.month,
                                                                       self.day,
                                                                       self.hour,
                                                                       self.filename)
        return s3_file_path

    def requires(self):
        s3_file_path = self.get_s3_file_path()
        log.info(f'checking for existing file on s3: {s3_file_path}')
        s3 = self.get_s3fs()
        if not s3.exists(s3_file_path):
            return DownloadOneFileFromHDFSRequest(env_config=self.env_config,
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
        local_directory = 'data'
        s3_file_path = self.get_s3_file_path()
        log.info(f'uploading file to s3: {s3_file_path}')
        s3 = self.get_s3fs()
        if not s3.exists(s3_file_path):
            s3.put(os.path.join(local_directory, self.filename), s3_file_path)
        else:
            log.warning(f'file already exists on s3 - skipping upload for {s3_file_path}')
        self.mark_complete()


class DeleteLocalFileRequest(TaskTemplate):

    def requires(self):
        return UploadOneFiletoS3Request(env_config=self.env_config,
                                        year=self.year,
                                        month=self.month,
                                        day=self.day,
                                        hour=self.hour,
                                        filename=self.filename,
                                        )

    def run(self):
        local_file_path = os.path.join('data', self.filename)
        log.info('deleting local file: ' + local_file_path)
        try:
            os.remove(local_file_path)
        except FileNotFoundError as e:
            log.warning(e)
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
                    DeleteLocalFileRequest(env_config=self.env_config,
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
    print(run_timestamp)

    luigi.build([
        Find_All_Files_For_Hour(
            env_config=pipeline_config.prod_config,
            year='2020',
            month='02',
            day='01',
            hour='23',
            filename=''
        )
    ],
        workers=10,
        local_scheduler=True,
        detailed_summary=False
    )
