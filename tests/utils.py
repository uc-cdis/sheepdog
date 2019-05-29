import json
import os
import time
import uuid

from authlib.common.encoding import to_unicode
import jwt

from cdislogging import get_logger

logger = get_logger(__name__, log_level="info")


def read_file(filename):
    """Read the contents of a file in the tests directory."""
    root_dir = os.path.dirname(os.path.realpath(__file__))
    with open(os.path.join(root_dir, filename), "r") as f:
        return f.read()


class JWTResult(object):
    """
    Just copy-pasted from Fence code so that
    we don't need to import Fence

    Just a container for the results necessary to keep track of from generating
    a JWT.
    """

    def __init__(self, token=None, kid=None, claims=None):
        self.token = token
        self.kid = kid
        self.claims = claims


def issued_and_expiration_times(seconds_to_expire):
    """
    Just copy-pasted from Fence code so that
    we don't need to import Fence

    Return the times in unix time that a token is being issued and will be
    expired (the issuing time being now, and the expiration being
    ``seconds_to_expire`` seconds after that). Used for constructing JWTs
    Args:
        seconds_to_expire (int): lifetime in seconds
    Return:
        Tuple[int, int]: (issued, expired) times in unix time
    """
    iat = int(time.time())
    exp = iat + int(seconds_to_expire)
    return (iat, exp)


def generate_signed_access_token(
    kid,
    private_key,
    user,
    expires_in,
    scopes,
    iss=None,
    forced_exp_time=None,
    client_id=None,
    linked_google_email=None,
):
    """
    Just copy-pasted from Fence code so that
    we don't need to import Fence

    Generate a JWT access token and output a UTF-8
    string of the encoded JWT signed with the private key.
    Args:
        kid (str): key id of the keypair used to generate token
        private_key (str): RSA private key to sign and encode the JWT with
        user (fence.models.User): User to generate ID token for
        expires_in (int): seconds until expiration
        scopes (List[str]): oauth scopes for user
    Return:
        str: encoded JWT access token signed with ``private_key``
    """
    headers = {"kid": kid}
    iat, exp = issued_and_expiration_times(expires_in)
    # force exp time if provided
    exp = forced_exp_time or exp
    sub = str(user.id)
    jti = str(uuid.uuid4())
    if not iss:
        try:
            iss = config.get("BASE_URL")
        except RuntimeError:
            raise ValueError(
                "must provide value for `iss` (issuer) field if"
                " running outside of flask application"
            )

    claims = {
        "pur": "access",
        "aud": scopes,
        "sub": sub,
        "iss": iss,
        "iat": iat,
        "exp": exp,
        "jti": jti,
        "context": {
            "user": {
                "name": user.username,
                "is_admin": user.is_admin,
                "projects": dict(user.project_access),
                "google": {"proxy_group": user.google_proxy_group_id},
            }
        },
        "azp": client_id or "",
    }

    # only add google linkage information if provided
    if linked_google_email:
        claims["context"]["user"]["google"][
            "linked_google_account"
        ] = linked_google_email

    logger.info("issuing JWT access token with id [{}] to [{}]".format(jti, sub))
    logger.debug("issuing JWT access token\n" + json.dumps(claims, indent=4))

    token = jwt.encode(claims, private_key, headers=headers, algorithm="RS256")
    token = to_unicode(token, "UTF-8")

    # Browser may clip cookies larger than 4096 bytes
    if len(token) > 4096:
        raise JWTSizeError("JWT exceeded 4096 bytes")

    return JWTResult(token=token, kid=kid, claims=claims)
