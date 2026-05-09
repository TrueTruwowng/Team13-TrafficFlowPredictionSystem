#!/bin/bash

# --- CẤU HÌNH ---
KAFKA_PATH="/opt/kafka"
SPARK_PATH="/opt/spark"
DOCKER_COMPOSE_PATH="/home/dis/Project"
NODES=("master14" "worker141" "worker142")

echo "=================================================="
echo "🛑 ĐANG TẠM DỪNG HỆ THỐNG (AIRFLOW-SPARK-KAFKA-DOCKER) 🛑"
echo "=================================================="

# 1. Dừng Airflow
echo "1. Dừng Airflow..."
pkill -f "airflow"
echo "   -> Airflow: STOPPED"

# 2. Dừng Spark Cluster
echo "2. Dừng Spark Cluster..."
$SPARK_PATH/sbin/stop-all.sh
echo "   -> Spark: STOPPED"

# 3. Dừng Kafka Cluster trên 3 Node
echo "3. Dừng Kafka Cluster..."
for node in "${NODES[@]}"
do
    ssh $node "$KAFKA_PATH/bin/kafka-server-stop.sh"
    # Diệt tận gốc nếu Kafka vẫn lỳ lợm chạy ngầm
    ssh $node "ps ax | grep kafka | grep -v grep | awk '{print \$1}' | xargs -r sudo kill -9"
    echo "   -> Kafka @ $node: STOPPED"
done

# 4. Dừng Docker (Kafka UI)
echo "4. Dừng Docker Containers..."
if [ -d "$DOCKER_COMPOSE_PATH" ]; then
    cd "$DOCKER_COMPOSE_PATH"
    docker-compose down
    echo "   -> Docker: CLEANED UP"
    cd - > /dev/null
fi

echo "=================================================="
echo "✅ TẤT CẢ TÀI NGUYÊN ĐÃ ĐƯỢC GIẢI PHÓNG!"
echo "=================================================="