docker compose up -d --build

docker exec -it lab_1-broker1-1 kafka-topics \
  --create \
  --topic raw-data \
  --bootstrap-server broker1:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it lab_1-broker1-1 kafka-topics \
  --create \
  --topic processed-data \
  --bootstrap-server broker1:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it lab_1-broker1-1 kafka-topics \
  --create \
  --topic visualization \
  --bootstrap-server broker1:9092 \
  --partitions 1 \
  --replication-factor 1

docker exec -it lab_1-broker1-1 kafka-topics \
  --create \
  --topic ml-results \
  --bootstrap-server broker1:9092 \
  --partitions 1 \
  --replication-factor 1