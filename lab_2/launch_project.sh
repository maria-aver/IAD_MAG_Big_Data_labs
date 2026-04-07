# Single node

docker compose -f docker-compose.yml up -d --scale datanode=1
bash hadoop/hdfs_upload.sh
