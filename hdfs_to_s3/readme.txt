Configuraton steps required (all files referenced are in the current directory):

copy .okta-aws.template to .okta-aws
update .okta-aws with your correct okta configuration information

copy pipeline_config.py.template to pipeline_config.py
update pipeline_config.py with your http proxy user name and password

create an empty file called aws_credentials.config with one line:
[default]
