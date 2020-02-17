""" Wrapper script for awscli which handles Okta auth """
import os
from subprocess import call
import logging
from oktaawscli.version import __version__
from oktaawscli.okta_auth import OktaAuth
from oktaawscli.okta_auth_config import OktaAuthConfig
from oktaawscli.aws_auth import AwsAuth


def get_credentials(aws_auth, okta_profile, profile,
                    verbose, logger, totp_token, cache):
    """ Gets credentials from Okta """

    okta_auth_config = OktaAuthConfig(logger)
    okta = OktaAuth(okta_profile, verbose, logger, totp_token, okta_auth_config)

    _, assertion = okta.get_assertion()
    role = aws_auth.choose_aws_role(assertion)
    principal_arn, role_arn = role

    okta_auth_config.save_chosen_role_for_profile(okta_profile, role_arn)
    duration = okta_auth_config.duration_for(okta_profile)

    sts_token = aws_auth.get_sts_token(
        role_arn,
        principal_arn,
        assertion,
        duration=duration,
        logger=logger
    )
    access_key_id = sts_token['AccessKeyId']
    secret_access_key = sts_token['SecretAccessKey']
    session_token = sts_token['SessionToken']
    session_token_expiry = sts_token['Expiration']
    logger.info("Session token expires on: %s" % session_token_expiry)
    if not profile:
        exports = console_output(access_key_id, secret_access_key,
                                 session_token, session_token_expiry, verbose)
        if cache:
            cache = open(os.path.join(os.getcwd(), 'aws_credentials.config'), 'w')
            cache.write(exports)
            cache.close()
    else:
        aws_auth.write_sts_token(profile, access_key_id,
                                 secret_access_key, session_token)
    return access_key_id, secret_access_key, session_token, session_token_expiry


def console_output(access_key_id, secret_access_key, session_token, session_token_expiry, verbose):
    """ Outputs STS credentials to console """
    if verbose:
        print("Use these to set your environment variables:")
    exports = "\n".join([
        "[default]",
        "aws_access_key_id=%s" % access_key_id,
        "aws_secret_access_key=%s" % secret_access_key,
        "aws_session_token=%s" % session_token,
        "session_token_expiry=%s" % session_token_expiry
    ])

    return exports


def main(okta_profile, profile, verbose, version,
         debug, force, cache, awscli_args, token):
    """ Authenticate to awscli using Okta """
    if version:
        print(__version__)
        exit(0)
    # Set up logging
    logger = logging.getLogger('okta-awscli')
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler()
    handler.setLevel(logging.WARN)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if verbose:
        handler.setLevel(logging.INFO)
    if debug:
        handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)

    if not okta_profile:
        okta_profile = "default"
    aws_auth = AwsAuth(profile, okta_profile, verbose, logger)
    if not aws_auth.check_sts_token(profile) or force:
        if force and profile:
            logger.info("Force option selected, \
                getting new credentials anyway.")
        elif force:
            logger.info("Force option selected, but no profile provided. \
                Option has no effect.")
        return get_credentials(
            aws_auth, okta_profile, profile, verbose, logger, token, cache
        )

    if awscli_args:
        cmdline = ['aws', '--profile', profile] + list(awscli_args)
        logger.info('Invoking: %s', ' '.join(cmdline))
        call(cmdline)


if __name__ == "__main__":
    # pylint: disable=E1120
    main()
    # pylint: enable=E1120
