docker network rm MLSpark
docker network create MLSpark

docker-compose up -d --build namenode datanode
docker exec -it namenode ./init.sh

