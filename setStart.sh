#!/bin/sh
nginx -c nginx.conf -p /etc/nginx
/home/yy/anaconda3/envs/py37/bin/gunicorn -c /home/yy/Project/RewardPoint/gunicorn_config.py app:app
