import dateutil.parser
import datetime
from oktaawscli import okta_awscli
import configparser


def get_aws_credentials():
    try:
        config = configparser.ConfigParser()
        config.read('aws_credentials.config')
        aws_access_key_id = config['default']['aws_access_key_id']
        aws_secret_access_key = config['default']['aws_secret_access_key']
        aws_session_token = config['default']['aws_session_token']
        session_token_expiry = config['default']['session_token_expiry']
        expiration_datetime = dateutil.parser.parse(session_token_expiry)
        current_time = datetime.datetime.now(datetime.timezone.utc)
        time_diff = datetime.timedelta(minutes=5)
        if expiration_datetime - time_diff > current_time:
            return aws_access_key_id, aws_secret_access_key, aws_session_token, session_token_expiry
    except Exception:
        print('error reading aws_credentials.config file')

    print('credentials less than 5 minutes to expiration - getting new credentials')
    return okta_awscli.main(None,  # okta_profile Name of the profile to use in .okta-aws.
                            None,
                            # Name of the profile to store temporary credentials in ~/.aws/credentials.
                            # If profile doesn't exist, it will be created.
                            # If omitted, credentials will output to console.
                            False,  # verbose
                            False,  # version
                            False,  # debug
                            False,  # force
                            True,  # Cache the default profile credentials to .okta-credentials.cache
                            None,  # awscli_args
                            None,  # TOTP token from your authenticator app
                            )
