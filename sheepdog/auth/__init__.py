"""sheepdog.auth

The authutils to use will depend on the downstream dependency
and how it installs authutils.

eg:
``pip install git+https://git@github.com/NCI-GDC/authutils.git@1.2.3#egg=authutils``
or
``pip install git+https://git@github.com/uc-cdis/authutils.git@1.2.3#egg=authutils``

"""

from authutils import *
