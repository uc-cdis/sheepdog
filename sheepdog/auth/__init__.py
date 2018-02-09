"""sheepdog.auth

The authutils to use will depend on the downstream dependency
and how it installs authutils.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``

"""

from authutils import AuthDriver
from authutils import admin_auth
from authutils.token import current_token
# import modules from authutils
from authutils import dbgap
from authutils import require_auth
from authutils import roles
from authutils import set_global_user
from authutils import authorize_for_project
