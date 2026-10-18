# VietPulse

VietPulse là hệ thống big data theo dõi cảm xúc và xu hướng chủ đề của dư luận Việt Nam gần thời gian thực. Hệ thống thu thập bài viết trên báo điện tử, bình luận dưới bài báo và bài đăng trên diễn đàn VOZ, rồi phân tích xem chủ đề nào đang nóng lên và công chúng đang phản ứng tích cực hay tiêu cực.

Đây là milestone project của môn IT4043E Big Data Storage and Processing (HUST).

## Hệ thống trả lời được gì

- Chủ đề và nhân vật nào đang được nhắc đến nhiều, cảm xúc về chúng ra sao trong vài giờ gần nhất.
- Giọng điệu của bài báo khác phản ứng của người đọc ở đâu.
- Cảm xúc theo chủ đề và theo nguồn thay đổi thế nào qua nhiều tháng.

## Kiến trúc

Dự án dùng kiến trúc Lambda. Dữ liệu từ crawler đi vào Kafka, rồi tách làm hai nhánh:

- **Speed layer:** Spark Structured Streaming cho kết quả trong vài phút.
- **Batch layer:** Spark batch tính lại toàn bộ trên HDFS mỗi đêm để có số liệu chính xác.

Kết quả của hai nhánh được gộp theo mốc T: MongoDB giữ các view, Elasticsearch giữ tài liệu để tìm kiếm. Dashboard dùng Kibana và Streamlit. Toàn bộ hệ thống chạy trên Kubernetes (k3s).

**Công nghệ chính:** Kafka · Spark (PySpark) · HDFS · MongoDB · Elasticsearch · PhoBERT · k3s · Prometheus, Grafana

Kế hoạch chi tiết và tiến độ hằng tuần: [VietPulse-Plan-and-Tracker.md](VietPulse-Plan-and-Tracker.md)

## Cấu trúc repo

```text
common/      logic dùng chung cho crawler, speed layer và batch layer (envelope, làm sạch, chấm điểm)
contracts/   hợp đồng dữ liệu dạng máy đọc được (JSON Schema)
crawlers/    crawler báo điện tử, bình luận và VOZ
tests/       pytest
docs/        tài liệu kỹ thuật, kết quả khảo sát
docker/      file phụ cho docker-compose của môi trường dev
```

Các thư mục `jobs/` (Spark job), `api/`, `dashboards/` và `k8s/` được thêm khi tới mốc tương ứng trong kế hoạch.

## Bắt đầu

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[crawler,dev]"
pytest
python -m crawlers.sample --out data/samples   # crawl thử, xem docs/m1.5-nguon-du-lieu.md
```

## Môi trường dev

`docker-compose.yml` dựng Kafka, Spark, MongoDB, Elasticsearch và Kibana trên máy cá nhân. Cần Docker Desktop (hoặc Docker Engine kèm Compose v2), và cấp cho Docker ít nhất 8 GB RAM.

```bash
docker compose up -d --build
docker compose ps                                              # chờ các dịch vụ healthy, kafka-init thoát với mã 0
docker compose exec spark spark-submit docker/spark-dev/smoke_test.py   # in "Môi trường dev OK"
```

Lần chạy Spark đầu tiên tải jar connector nên mất vài phút; các lần sau dùng lại volume `ivy-cache`.

| Dịch vụ | Từ máy host | Từ container khác | Phiên bản |
| --- | --- | --- | --- |
| Kafka (KRaft) | `localhost:9094` | `kafka:9092` | 4.0.0 |
| Spark (local mode, Python 3.11) | UI `localhost:4040` khi có job | chạy bằng `docker compose exec spark ...` | 3.5.3 |
| MongoDB | `localhost:27017` | `mongodb:27017` | 7.0.14 |
| Elasticsearch | `localhost:9200` | `elasticsearch:9200` | 8.15.3 |
| Kibana | `localhost:5601` | | 8.15.3 |

- Repo được mount vào `/workspace` trong container Spark, nên sửa code trên máy là chạy được ngay.
- Bản dev không bật bảo mật và chỉ mở cổng trên `127.0.0.1`.
- `docker compose down` xóa dữ liệu Kafka (topic được tạo lại ở lần `up` sau). `docker compose down -v` xóa cả dữ liệu MongoDB và Elasticsearch.
- Bản dev không có HDFS: job Spark đọc ghi thư mục local, ví dụ `/workspace/data/`.

Quy trình nhánh, commit và pull request: [CONTRIBUTING.md](CONTRIBUTING.md)
