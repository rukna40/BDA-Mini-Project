## Commands: 
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
python3 scraper.py 
spark-submit --master local[4] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 recieve.py 
