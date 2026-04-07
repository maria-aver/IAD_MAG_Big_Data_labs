# Single node

docker compose -f docker-compose.yml up -d --build --scale datanode=1
bash hadoop/hdfs_upload.sh
docker exec -it spark-master spark-submit /app/app.py