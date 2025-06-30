rm -f ./Kafka/offset.txt
touch ./Kafka/offset.txt
echo "Đã reset lại file Kafka/offset.txt"

hdfs dfs -rm -r /odap
hdfs dfs -mkdir /odap
hdfs dfs -mkdir /odap/credit_card_transactions
hdfs dfs -mkdir /odap/checkpoint
echo "Đã reset lại HDFS: /odap"

rm -f ./Hadoop/last_push_timestamp.txt
touch ./Hadoop/last_push_timestamp.txt
echo "Đã reset lại file Hadoop/last_push_timestamp.txt"

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic credit_card_transactions --if-exists
echo "Đang đợi Kafka hoàn tất xóa topic..."

sleep 3

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic credit_card_transactions --partitions 3 --replication-factor 1 --if-not-exists

echo "Đã tạo lại topic transactions"
echo "Reset hoàn tất!"