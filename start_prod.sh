# Start with root user
# scl enable rh-python36 bash
# nohup sh start_prod.sh &
waitress-serve --call 'qa:app'