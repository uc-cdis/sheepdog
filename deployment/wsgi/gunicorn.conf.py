# /sheepdoog/bin/settings.py
wsgi_app = "bin.settings:application"
bind = "0.0.0.0:8000"
workers = 4
preload_app = True
user = "gen3"
group = "gen3"
timeout = 300
keepalive = 2
keepalive_timeout = 5
