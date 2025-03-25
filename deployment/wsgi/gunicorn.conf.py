wsgi_app = "bin.settings:application"
bind = "0.0.0.0:8000"
workers = 2
preload_app = False
user = "gen3"
group = "gen3"
timeout = 300
graceful_timeout = 45
keepalive = 10
pidfile = "/sheepdog/gunicorn.pid"
