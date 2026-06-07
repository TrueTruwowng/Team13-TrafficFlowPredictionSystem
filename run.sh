#!/bin/bash

# --- 1. LOAD BIẾN MÔI TRƯỜNG ---
# Kiểm tra nếu có file .env thì load vào để lấy KAFKA_CLUSTER_ID, API Keys...
if [ -f "/home/dis/Project/.env" ]; then
    export $(cat /home/dis/Project/.env | grep -v '^#' | xargs)
    echo "✅ Loaded environment variables from .env"
fi

# --- 2. CẤU HÌNH ĐƯỜNG DẪN ---
KAFKA_PATH="/opt/kafka"
SPARK_PATH="/opt/spark"
DOCKER_COMPOSE_PATH="/home/dis/Project"
PYTHON_ENV="/home/dis/myenv/bin/python3"

# --- 3. KIỂM TRA KAFKA_CLUSTER_ID ---
# Nếu biến KAFKA_CLUSTER_ID chưa được set trong .env hoặc bashrc, script sẽ báo lỗi
if [ -z "$KAFKA_CLUSTER_ID" ]; then
    echo "❌ Lỗi: Biến KAFKA_CLUSTER_ID chưa được thiết lập!"
    echo "Vui lòng chạy 'export KAFKA_CLUSTER_ID=...' hoặc thêm vào file .env"
    exit 1
fi

NODES=("master14" "worker141" "worker142")
EXT_IP=$(curl -s ifconfig.me)

echo "=================================================="
echo "🚀 INITIATING DATA ECOSYSTEM (Dynamic ID: $KAFKA_CLUSTER_ID) 🚀"
echo "=================================================="

# --- 4. KHỞI CHẠY DOCKER ---
echo "0. Restarting Docker Containers..."
if [ -d "$DOCKER_COMPOSE_PATH" ]; then
    cd "$DOCKER_COMPOSE_PATH"
    docker-compose down > /dev/null 2>&1
    docker-compose up -d
    echo "   -> Docker Containers: UP"
    cd - > /dev/null
fi

# --- 5. KHỞI CHẠY KAFKA CLUSTER ---
echo "1. Synchronizing and Starting Kafka Cluster..."
for node in "${NODES[@]}"
do
    # Kiểm tra Kafka đã chạy chưa
    K_PID=$(ssh $node "jps 2>/dev/null | grep -i 'Kafka' | awk '{print \$1}'")
    if [ -n "$K_PID" ]; then
        echo "   -> Kafka @ $node: already RUNNING (PID: $K_PID), skipping start"
        continue
    fi

    # --ignore-formatted: no-op nếu đã format rồi → không bao giờ xóa topic cũ
    echo "   [!] Formatting storage on $node (safe, idempotent)..."
    ssh $node "$KAFKA_PATH/bin/kafka-storage.sh format --ignore-formatted \
        -t $KAFKA_CLUSTER_ID \
        -c $KAFKA_PATH/config/kraft/server.properties"

    # Khởi chạy Kafka Server
    ssh $node "source ~/.bashrc; $KAFKA_PATH/bin/kafka-server-start.sh -daemon \
        $KAFKA_PATH/config/kraft/server.properties"

    sleep 3
    K_PID=$(ssh $node "jps 2>/dev/null | grep -i 'Kafka' | awk '{print \$1}'")
    if [ -n "$K_PID" ]; then
        echo "   -> Kafka @ $node: RUNNING (PID: $K_PID)"
    else
        echo "   ❌ Kafka @ $node: FAILED. Check $KAFKA_PATH/logs/server.log"
    fi
done

# --- 6. KHỞI CHẠY SPARK ---
echo "2. Starting Spark Cluster..."
# Dọn sạch bất kỳ Spark process nào đang chạy (kể cả từ /lib/spark)
/lib/spark/sbin/stop-all.sh > /dev/null 2>&1
$SPARK_PATH/sbin/stop-all.sh > /dev/null 2>&1
sleep 2
$SPARK_PATH/sbin/start-all.sh
echo "   -> Spark 3.5.1: Master & Workers started"

# --- 7. KHỞI CHẠY DASHBOARD ---
echo "3. Starting Dashboard Backend (port 8000)..."
BACKEND_DIR="/home/dis/Project/dashboard/backend"
nohup bash -c "cd '$BACKEND_DIR' && $PYTHON_ENV -m uvicorn app.main:app --host 0.0.0.0 --port 8001" > /tmp/backend.log 2>&1 &
echo "   -> Dashboard Backend: started (log: /tmp/backend.log)"

echo "   Starting Dashboard UI (port 3000)..."
UI_DIR="/home/dis/Project/dashboard/frontend"
NPM_BIN="/home/TrueTruwowng/.nvm/versions/node/v25.9.0/bin/npm"
nohup bash -c "cd '$UI_DIR' && $NPM_BIN start" > /tmp/ui_dashboard.log 2>&1 &
echo "   -> Dashboard UI: started (log: /tmp/ui_dashboard.log)"

echo "=================================================="
echo "✅ ALL SERVICES DEPLOYED SUCCESSFULLY!"
echo "=================================================="

# --- TRẠNG THÁI TIẾN TRÌNH ---
echo "--- MASTER PROCESS OVERVIEW ---"
jps | grep -E 'Master|Worker|Kafka'
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo "--------------------------------------------------"
echo "🔗 ACCESSIBLE INTERFACES:"
echo "--------------------------------------------------"
echo "⭐ Spark Master: http://$EXT_IP:8080"
echo "⭐ Kafka UI:     http://$EXT_IP:8085"
echo "⭐ Dashboard UI: http://$EXT_IP:3000"
echo "⭐ Grafana:      http://$EXT_IP:3001"
echo "⭐ Prometheus:   http://$EXT_IP:9090"
echo "=================================================="
