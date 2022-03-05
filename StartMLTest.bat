docker stop malwerml
docker rm malwerml
docker-compose -f docker-composeTestModel.yml up -d --build zookeeper-server kafka-server Producer App




