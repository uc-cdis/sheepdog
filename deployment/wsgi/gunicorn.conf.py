import multiprocessing

wsgi_app = "bin.settings:application"
bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2 + 1
worker_class = "gevent"
worker_connections = 1000
preload_app = True
user = "gen3"
group = "gen3"
timeout = 300
keepalive = 10
pidfile = "/var/run/gunicorn.pid"
accesslog = "/sheepdog/log/gunicorn/access.log"
errorlog = "/sheepdog/log/gunicorn/error.log"
loglevel = "debug"
capture_output = True
