# Start with root user
# nohup sh start_prod.sh &
waitress-serve --call 'qa:app'