import sys

sys.path.append("/var/www/sheepdog/")
sys.path.append("/sheepdog/")
from wsgi import app as application
