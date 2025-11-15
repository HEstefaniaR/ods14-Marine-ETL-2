set KAFKA_DIR=C:\Users\estefania\Documents\kafka\kafka_2.13-3.9.1
set PROJECT_DIR=C:\Users\estefania\Documents\ods14-Marine-ETL-2

cd %KAFKA_DIR%

bin\windows\zookeeper-server-start.bat config\zookeeper.properties
bin\windows\kafka-server-start.bat config\server.properties

bin\windows\kafka-topics.bat --create --if-not-exists ^
  --topic microplastics-metrics ^
  --bootstrap-server localhost:9092 ^
  --partitions 1 ^
  --replication-factor 1

cd %PROJECT_DIR%

start python kafka\consumer.py
timeout /t 2
python kafka\producer.py