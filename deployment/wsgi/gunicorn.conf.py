import multiprocessing

wsgi_app = "bin.settings:application"
bind = "0.0.0.0:8000"
workers = 4
worker_class = "gevent"
worker_connections = 100
preload_app = False
user = "gen3"
group = "gen3"
timeout = 300
keepalive = 10
pidfile = "/sheepdog/gunicorn.pid"
