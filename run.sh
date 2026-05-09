#!/bin/bash

# --- CẤU HÌNH ĐƯỜNG DẪN ---
KAFKA_PATH="/opt/kafka"
SPARK_PATH="/opt/spark"
AIRFLOW_HOME="$HOME/airflow"
# Thư mục chứa file docker-compose.yml của bạn
DOCKER_COMPOSE_PATH="/home/dis/Project" 

# Đường dẫn tới python trong môi trường ảo của bạn
PYTHON_ENV="/home/dis/myenv/bin/python3" 
AIRFLOW_BIN="/home/dis/myenv/bin/airflow"

NODES=("master14" "worker141" "worker142")
EXT_IP=$(curl -s ifconfig.me)

echo "=================================================="
echo "🚀 ĐANG KHỞI CHẠY HỆ SINH THÁI DATA (DOCKER-KAFKA-SPARK-AIRFLOW) 🚀"
echo "=================================================="

# 0. Khởi chạy Docker Containers (Kafka UI, etc.)
echo "0. Đang làm sạch và khởi động lại Docker Containers..."
if [ -d "$DOCKER_COMPOSE_PATH" ]; then
    cd "$DOCKER_COMPOSE_PATH"
    
    # Bước 1: Ép dừng và xóa sạch các container cũ liên quan đến file compose này
    # Lệnh này sẽ giải quyết triệt để lỗi "Conflict"
    docker-compose down > /dev/null 2>&1
    
    # Bước 2: Khởi chạy mới hoàn toàn
    docker-compose up -d
    
    echo "   -> Docker Containers: RESTARTED (Fresh state)"
    cd - > /dev/null
else
    echo "   ❌ Lỗi: Không tìm thấy thư mục Docker tại $DOCKER_COMPOSE_PATH"
fi

# 1. Khởi chạy Kafka trên 3 Node
echo "1. Đang bật Kafka Cluster..."
for node in "${NODES[@]}"
do
    ssh $node "$KAFKA_PATH/bin/kafka-server-start.sh -daemon $KAFKA_PATH/config/kraft/server.properties"
    echo "   -> Kafka @ $node: DONE"
done

# 2. Khởi chạy Spark Cluster
echo "2. Đang bật Spark Cluster..."
$SPARK_PATH/sbin/start-all.sh

# 3. Khởi chạy Airflow (Webserver + Scheduler)
echo "3. Đang bật Airflow (Port 8082)..."
export AIRFLOW_HOME=$AIRFLOW_HOME
# Chạy Webserver ở chế độ daemon (-D)
$AIRFLOW_BIN webserver -p 8082 -D
# Chạy Scheduler ở chế độ daemon (-D)
$AIRFLOW_BIN scheduler -D
echo "   -> Airflow components: DONE"

echo "=================================================="
echo "✅ TẤT CẢ DỊCH VỤ ĐÃ ĐƯỢC KÍCH HOẠT!"
echo "=================================================="

# Kiểm tra trạng thái nhanh
echo "--- TRẠNG THÁI TIẾN TRÌNH TẠI MASTER ---"
jps | grep -E 'Master|Worker|Kafka'
ps aux | grep -E 'airflow-webserver|airflow-scheduler' | grep -v grep | awk '{print $11}' | uniq
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo "--------------------------------------------------"
echo "🔗 TRUY CẬP CÁC GIAO DIỆN UI:"
echo "--------------------------------------------------"
echo "⭐ Spark Master: http://$EXT_IP:8080"
echo "⭐ Airflow UI:   http://$EXT_IP:8082"
echo "⭐ Kafka UI:     http://$EXT_IP:8085"
echo "=================================================="