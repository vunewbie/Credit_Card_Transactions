hdfs dfs -rm -r /odap /odap_checkpoint
hdfs dfs -mkdir /odap
echo "Đã reset lại HDFS: /odap"

rm -f ./src/last_push_timestamp.txt
touch ./src/last_push_timestamp.txt
echo "Đã reset lại file src/last_push_timestamp.txt"

kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transactions
echo "Đang đợi Kafka hoàn tất xóa topic..."

sleep 3

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transactions
echo "Đã tạo lại topic transactions"
