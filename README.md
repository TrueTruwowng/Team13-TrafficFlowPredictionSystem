# Team13-TrafficFlowPredictionSystem

# TrafficFlow Dashboard

Khung FE/BE cho dashboard traffic:

- `backend/`: FastAPI + DuckDB, đọc parquet từ Google Cloud Storage.
- `frontend/`: Next.js app router, gọi API backend để hiển thị dữ liệu.
- `docker-compose.yml`: dựng cả FE và BE cùng lúc.

## Cấu trúc

```text
dashboard/
  backend/
    app/
      core/
      db/
      routers/
      services/
      main.py
    requirements.txt
    .env.example
      Dockerfile
  frontend/
      app/
      components/
      lib/
      package.json
      tsconfig.json
    .env.example
      Dockerfile
    docker-compose.yml
```

  ## Chạy bằng Docker Compose

  ```powershell
  cd e:\TrafficFlow\dashboard
  docker compose up --build -d
  ```

  Lần sau khi chỉ sửa source code, chạy:

  ```powershell
  docker compose up -d
  ```

  Theo dõi log hot-reload:

  ```powershell
  docker compose logs -f backend frontend
  ```

  Lưu ý: không cần `--build` mỗi lần sửa code. Chỉ dùng `--build` khi thay đổi Dockerfile hoặc dependency (`requirements.txt`, `package.json`).

  Sau đó:

  - Frontend: `http://localhost:3000`
  - Backend: `http://localhost:8000`

  ## Chạy backend riêng

```powershell
cd e:\TrafficFlow\dashboard\backend
e:/TrafficFlow/.venv/Scripts/python.exe -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

  ## Chạy frontend riêng

  ```powershell
  cd e:\TrafficFlow\dashboard\frontend
  npm install
  npm run dev
  ```

## Biến môi trường backend

- `GCS_BUCKET`: tên bucket Google Cloud Storage.
- `GCS_PREFIX`: prefix thư mục dữ liệu trong bucket.
- `GCP_PROJECT`: project id, nếu cần cho `google-cloud-storage`.
- `DUCKDB_PATH`: đường dẫn DB DuckDB local, mặc định in-memory.
- `CORS_ORIGINS`: danh sách origin cho Next.js, mặc định `http://localhost:3000,http://127.0.0.1:3000`.

Backend sẽ tự đọc file `dashboard/backend/.env` khi khởi động.

## Biến môi trường frontend

- `NEXT_PUBLIC_API_BASE_URL`: URL backend, mặc định `http://localhost:8000`.

## API theo ngày

- `GET /datasets/by-date?target_date=2026-05-29`
- `GET /datasets/by-date/data?target_date=2026-05-29` (trả toàn bộ rows của ngày đó)
- `GET /datasets/by-range?start_date=2026-05-01&end_date=2026-05-07`

