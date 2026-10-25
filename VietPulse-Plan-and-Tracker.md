# VietPulse: Plan and Tracker

> Cập nhật lần cuối: 18/10/2026 · Trưởng nhóm: Thái · Thành viên: Thái, Thạch, Thắng, Quang, Đạt

Tài liệu này gồm hai phần: kế hoạch (thiết kế và lịch 10 tuần) và phần theo dõi tiến độ hằng tuần ở cuối file.

## 1. Mục tiêu và phạm vi

Trong 10 tuần, nhóm 5 người xây VietPulse: pipeline Lambda trên k3s phân tích cảm xúc và chủ đề từ báo điện tử, bình luận dưới bài báo và diễn đàn VOZ, kèm báo cáo 4 phần theo guideline môn IT4043E. Bình luận YouTube là phần tùy chọn.

**Trong phạm vi:**

- 3 nguồn bắt buộc: bài báo, bình luận dưới bài báo, VOZ, kèm backfill lịch sử.
- Speed layer, batch layer, serving layer gộp theo mốc T, dashboard Kibana và Streamlit.
- Đủ 6 nhóm yêu cầu Spark của guideline.
- Triển khai trên k3s, giám sát bằng Prometheus, Grafana và Loki.

**Tùy chọn:**

- Bình luận YouTube, chỉ làm khi các việc bắt buộc của M4 đã xong. Thiết kế vẫn chừa sẵn chỗ (giá trị source, Kafka topic) để thêm nguồn này mà không phải sửa pipeline.

**Ngoài phạm vi:**

- Huấn luyện PhoBERT từ đầu; nhóm chỉ fine-tune hoặc dùng model có sẵn.
- Ứng dụng cho người dùng cuối, đăng nhập và phân quyền phức tạp.
- Triển khai trên cloud trả phí.

**Tiêu chí thành công** (mục tiêu đề xuất, đo lại khi chạy thật):

| Tiêu chí | Mục tiêu |
| --- | --- |
| Độ trễ speed layer, từ lúc crawl đến dashboard | dưới 5 phút |
| Chuỗi batch hằng đêm | xong trước 7h sáng |
| Khối lượng master dataset | từ vài triệu bản ghi trở lên |
| Mất dữ liệu khi kill pod Spark | 0 bản ghi, kiểm bằng đối soát |
| Lessons Learned | mỗi thành viên ít nhất 1 bài, phủ ít nhất 6 nhóm chủ đề |

## 2. Kiến trúc tổng quan

Kafka là điểm vào duy nhất. Speed layer cho kết quả nhanh nhưng gần đúng, batch layer tính lại đầy đủ mỗi đêm, và serving layer gộp hai view theo mốc T. HDFS giữ master dataset. Ở lớp phục vụ, MongoDB giữ các view còn Elasticsearch giữ tài liệu để tìm kiếm.

```mermaid
flowchart TD
    A["Báo điện tử"] --> C["Crawler trên k3s"]
    B["Bình luận dưới bài báo"] --> C
    V["Diễn đàn VOZ"] --> C
    Y["Bình luận YouTube<br/>(tùy chọn)"] -.-> C
    C --> K[("Kafka")]
    K -->|đọc stream| S["Speed layer<br/>Spark Structured Streaming"]
    K -->|ghi nguyên trạng| H[("HDFS<br/>master dataset")]
    H --> BL["Batch layer<br/>Spark batch hằng đêm"]
    S -->|real-time view| M[("MongoDB<br/>view phục vụ")]
    BL -->|batch view| M
    S -->|tài liệu| ES[("Elasticsearch<br/>tìm kiếm")]
    BL -->|tài liệu| ES
    M --> API["API gộp theo mốc T"]
    ES --> API
    API --> ST["Streamlit"]
    ES --> KB["Kibana"]
```

## 3. Hợp đồng dữ liệu

Mọi thành phần giao tiếp qua 4 hợp đồng dưới đây. Đổi một hợp đồng phải qua PR được người phụ trách của cả hai phía đồng ý và Thái review.

### Envelope chung cho mọi bản ghi trong Kafka

| Trường | Kiểu | Ý nghĩa |
| --- | --- | --- |
| source | string | news_article, news_comment, voz_post; youtube_comment nếu làm phần tùy chọn |
| record_id | string | id gốc ở nguồn kèm tiền tố trang, ví dụ `vnexpress:5133904`; key Kafka = `source:record_id` |
| event_time | timestamp UTC | thời điểm bài hoặc bình luận được viết |
| ingest_time | timestamp UTC | thời điểm crawler lấy về |
| schema_version | int | tăng khi đổi cấu trúc payload |
| payload | JSON | trường riêng của từng nguồn |

Mọi thời gian lưu theo UTC; chỉ đổi sang giờ Việt Nam khi hiển thị. Bản máy đọc được của envelope là `contracts/envelope.schema.json`, bản code là `common/envelope.py`.

### Kafka topics

Số partition và thời gian giữ là giá trị khởi điểm, chỉnh sau khi đo.

| Topic | Partition | Giữ lại | Ai đọc |
| --- | --- | --- | --- |
| raw.news.articles | 3 | 7 ngày | raw-ingest, speed-sentiment |
| raw.news.comments | 6 | 7 ngày | raw-ingest, speed-sentiment |
| raw.forum.posts | 3 | 7 ngày | raw-ingest, speed-sentiment |
| raw.youtube.comments (tùy chọn) | 3 | 7 ngày | raw-ingest, speed-sentiment |
| raw.backfill | 6 | 2 ngày | raw-ingest |
| dlq.raw | 1 | 14 ngày | người vận hành |
| alerts.sentiment | 1 | 7 ngày | dịch vụ cảnh báo |

### Thư mục HDFS

```text
/vietpulse
├── raw/source=…/event_date=…   nguyên trạng từ Kafka
├── clean/       bảng đã khử trùng, chuẩn hóa, gắn nhãn
├── curated/     chỉ số ngày, khoảng cách, đồ thị thực thể
├── reference/   tag_alias, từ điển thực thể, taxonomy
├── models/      PhoBERT (ONNX), model MLlib
└── checkpoints/ checkpoint của job streaming
```

Bảng trong clean và curated được đăng ký ở Hive Metastore, partition theo event_date. Bảng bài viết và bình luận được bucket theo article_id để join không cần shuffle.

### Lớp phục vụ: MongoDB và Elasticsearch

MongoDB (database `vietpulse`) giữ các view mà API đọc. Elasticsearch chỉ giữ tài liệu để tìm kiếm toàn văn và vẽ Kibana; nếu mất có thể dựng lại từ /clean.

| Kho | Tên | Nội dung | Ghi bởi | Thời gian giữ |
| --- | --- | --- | --- | --- |
| MongoDB | rt_window | tổng hợp cảm xúc theo cửa sổ cố định 5 phút | speed-sentiment | xóa phần trước mốc T sau mỗi lần batch-publish |
| MongoDB | batch_daily | chỉ số cảm xúc theo ngày | batch-publish | lâu dài |
| MongoDB | meta | mốc T của lần batch gần nhất | batch-publish | lâu dài |
| Elasticsearch | vp-docs | bài và bình luận đầy đủ để tìm kiếm | speed-sentiment (bản tạm), batch-publish (bản chuẩn) | lâu dài |
| Elasticsearch | vp-dlq | bản ghi lỗi kèm lý do, để xem trên Kibana | speed-sentiment | 3 ngày (ILM) |

API gộp đọc meta để lấy mốc T, lấy batch_daily cho phần trước T và rt_window cho phần sau T.

## 4. Đặc tả Spark job

Hệ thống có 6 job: 2 job streaming chạy liên tục và 4 job batch. Mọi job import logic xử lý từ thư mục `common/`, để speed layer và batch layer không lệch nhau.

| Job | Kiểu và lịch | Đọc → Ghi | Đảm bảo | Phụ trách |
| --- | --- | --- | --- | --- |
| raw-ingest | streaming, ghi file mỗi 5–15 phút | Kafka raw.* và raw.backfill → /raw | không mất, không trùng (checkpoint + file sink) | Quang |
| speed-sentiment | streaming, micro-batch vài chục giây | Kafka raw.* → MongoDB rt_window, vp-docs, alerts.sentiment, dlq.raw, vp-dlq | watermark 6 giờ, cửa sổ cố định 5 phút, bỏ trùng theo record_id, upsert idempotent | Quang |
| batch-clean | hằng đêm, 1h | /raw của các ngày có dữ liệu mới → /clean | ghi đè theo partition, chạy lại an toàn | Đạt |
| batch-analytics | hằng đêm, sau batch-clean | /clean → /curated | ghi đè theo partition | Đạt, Thái |
| batch-publish | hằng đêm, sau batch-analytics | /curated → MongoDB batch_daily và meta, vp-docs | mốc T chỉ cập nhật ở bước cuối | Đạt |
| reference-refresh | hằng tuần | /clean → /reference | nhóm duyệt trước khi áp dụng | Thái |

**Điều phối batch:** một CronJob chạy lúc 1h, submit lần lượt batch-clean → batch-analytics → batch-publish và chờ từng job xong. Nếu một bước lỗi, chuỗi dừng và mốc T giữ nguyên. Real-time view chưa bị xóa nên dashboard vẫn đúng.

**Dữ liệu đến muộn:** batch-clean không chỉ chạy partition hôm qua mà chạy lại mọi partition có bản ghi mới đến trong ngày. Nhờ ghi đè theo partition, chạy lại bao nhiêu lần cũng cho cùng kết quả.

### Speed layer: watermark, cửa sổ, khử trùng và DLQ

Chốt ở Task 6 (24/10/2026). speed-sentiment gồm hai query cùng đọc các topic raw.*, mỗi query một checkpoint riêng:

| Query | Làm gì | Ghi vào | Làm ở |
| --- | --- | --- | --- |
| A · tài liệu, không giữ state | kiểm tra hợp lệ; bản ghi lỗi sang DLQ; bản ghi tốt được chấm sentiment rồi upsert theo record_id | vp-docs, dlq.raw, vp-dlq, alerts.sentiment | M2.3; checkpoint 26/10 chỉ cần query này |
| B · tổng hợp, giữ state | lọc bản ghi hợp lệ, watermark 6 giờ trên event_time, bỏ trùng theo record_id, đếm theo cửa sổ cố định 5 phút | MongoDB rt_window, upsert qua foreachBatch | M3.4 |

**Watermark 6 giờ** (nâng từ 2 giờ ngày 24/10/2026 theo số đo). Watermark so lúc bài hoặc bình luận được viết (event_time) với lúc Spark nhận được, không so với giờ đăng bài. Số đo trên 490 bình luận Tuổi Trẻ ngày 24/10 (docs/m1.5-nguon-du-lieu.md):

| Chỉ số | Chờ duyệt | Tới Spark, tính cả lịch quét lại bình luận |
| --- | --- | --- |
| Trung vị | 39 phút | 57 phút |
| p95 | 153 phút | 221 phút |
| Trễ quá 2 giờ | 10,0% | 16,7% |
| Trễ quá 6 giờ | 1,4% | 2,2% |

Watermark 2 giờ sẽ bỏ khoảng 1/6 bình luận Tuổi Trẻ khỏi rt_window; 6 giờ chỉ bỏ 2,2%. VnExpress không trả lúc duyệt nên chưa đo được phần chờ duyệt; phần chờ tới lần quét kế tiếp tối đa 60 phút. Bản ghi trễ quá 6 giờ vẫn vào vp-docs và HDFS, chỉ thiếu trong rt_window; batch đêm bù lại. Đo lại sau khi crawler chạy thật vài tuần.

**Cửa sổ cố định 5 phút, không dùng cửa sổ trượt.** Mỗi bản ghi chỉ thuộc một ô nên cộng dồn được và gộp đúng với batch_daily tại mốc T. Số "15 phút gần nhất" hay đường làm mượt được tính khi đọc, ở API, Streamlit và luật cảnh báo. Kibana vẽ từ vp-docs, làm mượt bằng `moving_average` trong Lens. Không ghi kết quả cửa sổ vào Elasticsearch, vì sink Elasticsearch chỉ nhận chế độ append nên mỗi ô chỉ hiện ra sau khi watermark trôi qua.

**Khử trùng.** Crawler quét lại bình luận của mỗi bài trong 48 giờ, nên crawler phải nhớ các record_id đã gửi và chỉ gửi bản ghi mới. Spark vẫn bỏ trùng bằng `dropDuplicatesWithinWatermark("record_id")` làm lớp an toàn; bản trùng đến muộn hơn watermark thì đã bị bỏ sẵn. vp-docs upsert theo record_id nên không bị trùng.

**DLQ.** Chỉ speed-sentiment ghi DLQ. raw-ingest vẫn ghi mọi bản ghi xuống HDFS nguyên trạng, batch-clean lọc lại bằng cùng luật.

- Bản ghi bị đưa vào DLQ khi: JSON hỏng; thiếu một trong 6 trường của envelope; source không hợp lệ hoặc không khớp topic; event_time hay ingest_time không đọc được; event_time ở tương lai quá 1 giờ, thường do sai múi giờ; payload không phải object. Luật giống `validate_envelope` trong `common/envelope.py`, nhưng viết bằng biểu thức Spark SQL để không phải gọi Python cho từng bản ghi.
- Mỗi bản ghi DLQ gồm: raw_value (chuỗi gốc), error (lý do), source_topic, partition, offset, kafka_timestamp, detected_at.
- dlq.raw giữ 14 ngày. vp-dlq do chính query A ghi, ILM tự xóa sau 3 ngày; không cần consumer riêng.

## 5. Triển khai

Toàn bộ hệ thống chạy trên k3s, chia thành 6 namespace. Hai cấu hình single-node và multi-node dùng chung manifest qua Kustomize.

| Namespace | Thành phần | Cách cài |
| --- | --- | --- |
| ingestion | 3 crawler (Deployment, CronJob); crawler YouTube nếu làm phần tùy chọn | manifest tự viết |
| messaging | Kafka chế độ KRaft | Strimzi operator |
| storage | HDFS NameNode, DataNode; Hive Metastore + PostgreSQL | Helm chart, StatefulSet |
| processing | 6 Spark job | Spark Operator |
| serving | MongoDB, Elasticsearch, Kibana, API FastAPI, Streamlit | MongoDB Community Operator, ECK operator, Deployment |
| monitoring | Prometheus, Grafana, Alertmanager, Loki | kube-prometheus-stack, Helm chart Loki |

- **Image:** 3 image (crawler, spark, api) được build bằng GitHub Actions và đẩy lên GHCR. Spark image cài sẵn jar Kafka, GraphFrames, Elasticsearch, MongoDB Spark Connector cùng thư viện Python.
- **Cấu hình:** thư mục `k8s/base` cùng hai overlay single-node và multi-node.
- **Bí mật:** mật khẩu MongoDB và Elasticsearch (và API key YouTube nếu làm phần tùy chọn) để trong Secret, không commit lên GitHub.
- **Truy cập:** Kibana, Grafana, API và Streamlit mở qua Traefik, là ingress có sẵn trong k3s. HDFS và MongoDB chỉ mở trong cụm.
- **Ổ đĩa:** k3s dùng local-path, nên pod giữ dữ liệu bị ghim vào một máy. Cấu hình multi-node dựa vào replication của Kafka, HDFS và Elasticsearch, cùng replica set của MongoDB.
- **Môi trường dev:** `docker-compose.yml` chạy Kafka, Spark, MongoDB và Elasticsearch trên máy cá nhân để viết code trước khi lên k3s.

**Máy chạy cụm** (kết quả M1.3): cụm multi-node 5 máy, tổng 184 GB RAM và 40 nhân CPU.

| Máy | CPU | RAM | GPU | Ổ trống | Vai trò trong cụm |
| --- | --- | --- | --- | --- | --- |
| Thạch | 8 nhân, 16 luồng | 16 GB | RTX 4060 | 200 GB SSD | Quản lý hệ thống: k3s server, Prometheus, Grafana |
| Thắng | 4 nhân, 8 luồng | 8 GB | Intel Iris Xe | chưa ghi | Thu thập và giao diện: crawler, API, dashboard |
| Quang | 8 nhân, 16 luồng | 16 GB | RTX 4070 Ti Super | 300 GB | Xử lý batch: HDFS (dữ liệu thô), Spark batch |
| Đạt | 10 nhân, 16 luồng | 16 GB | RTX 4060 | chưa ghi | Xử lý real-time: Spark Streaming, sentiment |
| Thái | 10 nhân, 20 luồng | 128 GB | RTX 2080 | 300 GB | Lưu trữ và tính toán chính: MongoDB, Elasticsearch, HDFS, hỗ trợ Spark tác vụ nặng |

## 6. Kiểm thử và giám sát

Mỗi lớp của pipeline có kiểm thử riêng. Mọi kết quả đo được lưu lại làm số liệu cho Lessons Learned.

| Loại | Làm gì | Khi nào | Phụ trách |
| --- | --- | --- | --- |
| Unit | pytest cho `common/` (làm sạch, tách từ, gắn nhãn, chấm điểm) với SparkSession local | mỗi PR, qua GitHub Actions | người viết code |
| Tích hợp | trên Compose: crawler mẫu → Kafka → job → MongoDB và Elasticsearch, so số bản ghi ở hai đầu | trước khi merge một job mới | Quang |
| Chất lượng dữ liệu | kiểm schema, giá trị rỗng, bản trùng, tỉ lệ bản ghi vào dlq | mỗi đêm, trong batch-clean | Đạt |
| Đối soát | so batch view với real-time view của cùng một ngày | mỗi đêm, sau batch-publish | Thái |
| Chịu lỗi | kill pod Spark, Kafka broker, DataNode rồi kiểm không mất bản ghi | tuần 8–9 | Quang |
| Hiệu năng | so execution plan và thời gian chạy trước, sau tối ưu (bucketing, broadcast, cache) | tuần 7–9 | Đạt |
| Model | precision của gắn nhãn, F1 của sentiment trên mẫu gán tay | tuần 7 | Thái |

**Giám sát** (Thạch phụ trách):

- **Spark streaming:** số offset đang tụt, tốc độ vào và tốc độ xử lý, thời gian mỗi batch. Đẩy sang Prometheus qua StreamingQueryListener.
- **Kafka:** Kafka Exporter của Strimzi và dung lượng đĩa của broker.
- **Batch:** thời gian chạy từng job, và mốc T có tiến lên mỗi ngày không.
- **Crawler:** thời điểm gửi bản ghi gần nhất của từng trang, đẩy lên Prometheus.
- **DLQ:** tốc độ vào dlq.raw so với tổng tốc độ vào raw.*, lấy từ Kafka Exporter.

**Cảnh báo** (ngưỡng khởi điểm):

- raw-ingest dừng quá 30 phút.
- Số offset tụt tăng liên tục quá 15 phút.
- Chuỗi batch đêm thất bại.
- Đĩa của bất kỳ node nào vượt 80%.
- Bản ghi vào dlq.raw chiếm trên 5% tổng số bản ghi trong 15 phút. Dùng tỉ lệ thay vì số tuyệt đối vì cả hệ thống chỉ có vài bản ghi mỗi phút.
- Một trang không có bản ghi mới quá 60 phút, chỉ tính từ 7h đến 23h giờ Việt Nam. Khi trang đổi HTML, crawler thường trả về rỗng chứ không sinh bản ghi lỗi, nên cảnh báo DLQ không bắt được.

## 7. Kịch bản demo

Buổi demo có 3 phần. Phần 1 chạy dữ liệu thật để chứng minh luồng vào. Phần 2 là luồng chính: dùng dữ liệu chuẩn bị sẵn để chủ động tạo tải lớn và các edge case. Phần 3 trình bày kết quả batch của đêm trước, vì batch quá nặng để chạy trực tiếp.

**Phần 1 · Ingestion thật (khoảng 2 phút).** Mở log của một pod crawler RSS báo điện tử, nguồn nhẹ và ít bị chặn. Cho giảng viên thấy bản ghi mới đang được kéo về và đẩy vào raw.news.articles. Xong thì scale crawler về 0 để giải phóng RAM cho phần 2.

**Phần 2 · Edge case với khối lượng lớn (luồng chính).** Một script Python đọc file Parquet vài trăm nghìn bình luận đã chuẩn bị sẵn và bắn dồn dập vào raw.news.comments. Phần lớn bản ghi được đổi event_time thành thời điểm demo; một phần được cố ý cài các edge case dưới đây. Trong lúc chạy, mở Kibana để thấy vp-docs tăng liên tục và Grafana để thấy offset tụt tăng lên rồi giảm về.

| Edge case | Cách tạo trong file Parquet | Kết quả mong đợi |
| --- | --- | --- |
| Tải dồn | gửi cả file trong vài phút, không giãn cách | offset tụt tăng rồi giảm về; số bản ghi trong Kafka khớp với /raw |
| Bản trùng | gửi lại cùng source + record_id nhiều lần | upsert idempotent: vp-docs và rt_window không đếm trùng |
| Đến muộn | event_time cũ hơn watermark 6 giờ | rt_window không tính bản ghi này; raw-ingest vẫn ghi xuống /raw nên batch đêm tính đủ |
| Sai schema | thiếu trường bắt buộc, sai kiểu, JSON hỏng | bản ghi bị tách vào dlq.raw, job không dừng |
| Văn bản khó | emoji, teencode, không dấu, rất dài, rỗng, lẫn tiếng Anh | được chuẩn hóa rồi chấm điểm bằng PhoBERT ONNX qua UDF; văn bản rỗng không làm job lỗi |

**Phần 3 · Batch layer (kết quả đã chạy trước).** Batch chạy GraphFrames, LDA và PhoBERT đầy đủ nên quá tốn tài nguyên để chạy trực tiếp. Mở bảng curated trên HDFS hoặc Streamlit với kết quả của đêm trước để so sánh khoảng cách giữa báo chí và dư luận.

## 8. Quyết định kiến trúc (ADR)

Cả 8 quyết định đã chốt ở M1.7 (18/10/2026). Mỗi dòng ghi lý do chính để đưa thẳng vào phần Architecture and Design của báo cáo.

| Quyết định | Lựa chọn | Lý do chính | Trạng thái |
| --- | --- | --- | --- |
| Kiến trúc | Lambda | GraphFrames, huấn luyện MLlib và bucketing cần batch; bình luận đến muộn; đổi model chỉ cần chạy lại batch | Đã chốt |
| Nền tảng | k3s | nhẹ, ghép được nhiều máy, đáp ứng yêu cầu Kubernetes | Đã chốt |
| Message queue | Kafka KRaft qua Strimzi | giữ log cho nhiều consumer, replay được, operator lo vận hành | Đã chốt |
| Lưu trữ dài hạn | HDFS | bám sát chương 3 của môn; MinIO bản community đã ngừng bảo trì từ 12/2025 nên không chọn | Đã chốt |
| NoSQL phục vụ truy vấn | MongoDB giữ view, Elasticsearch để tìm kiếm | MongoDB upsert theo khóa, hợp với ghi idempotent và API đọc theo khóa; Elasticsearch tìm kiếm toàn văn tiếng Việt, có sẵn Kibana | Đã chốt |
| Catalog | Hive Metastore + PostgreSQL | bucketBy chỉ chạy với saveAsTable, cần catalog dùng chung | Đã chốt |
| Điều phối batch | CronJob chạy script tuần tự | ít thành phần; chuyển sang Airflow nếu còn thời gian | Đã chốt |
| Sentiment ở speed layer | PhoBERT bản nhẹ, chạy ONNX trên CPU | giữ độ trễ thấp; model đầy đủ chạy ở batch layer | Đã chốt |
## 9. Phân vai

Thái là trưởng nhóm: review mọi PR trước khi merge, điều phối báo cáo và slide, và kiêm vai NLP/ML. Bốn thành viên còn lại ngang nhau, mỗi người sở hữu một lớp của pipeline và các bài Lessons Learned của lớp đó. Khi Thái là người viết code, một thành viên khác review PR đó.

| Thành viên | Vai | Phụ trách | Nhóm Lessons Learned chính |
| --- | --- | --- | --- |
| Thái | Trưởng nhóm · NLP/ML | review mọi PR, điều phối báo cáo; PhoBERT ONNX, gắn tag/chủ đề/thực thể, MLlib (LDA, baseline), GraphFrames | Data Quality & Testing |
| Thạch | Platform | k3s, operator, CI/CD image, Kustomize, monitoring | Scaling, Monitoring & Debugging, Security & Governance |
| Thắng | Ingestion | 3 crawler (bài báo, bình luận báo, VOZ), envelope, Kafka topics, backfill, dlq; crawler YouTube nếu làm phần tùy chọn | Data Ingestion, System Integration |
| Quang | Streaming và serving | raw-ingest, speed-sentiment, watermark, exactly-once, collection MongoDB, index Elasticsearch, API gộp, dashboard | Stream Processing, Fault Tolerance |
| Đạt | Batch và lưu trữ | HDFS, Hive Metastore, batch-clean, batch-analytics, batch-publish, join, tối ưu | Data Processing with Spark, Data Storage, Performance Optimization |

## 10. Lịch thực thi (ước tính 10 tuần)

Chưa có ngày thuyết trình nên lịch tính theo tuần tương đối: tuần 1 bắt đầu từ buổi kick-off. Nhóm đóng băng tính năng cuối tuần 8 để dành tuần 9–10 cho kiểm thử, báo cáo và tập demo. Khi có ngày cụ thể, ghi ngày bắt đầu và kết thúc vào từng tuần ở phần theo dõi.

| Giai đoạn / mốc | T1 | T2 | T3 | T4 | T5 | T6 | T7 | T8 | T9 | T10 |
| --- | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: | :-: |
| Học + môi trường dev | ■ | ■ |  |  |  |  |  |  |  |  |
| GĐ0 · Hạ tầng k3s | ■ | ■ |  |  |  |  |  |  |  |  |
| M1 · Thiết kế chốt | ◆ |  |  |  |  |  |  |  |  |  |
| GĐ1 · Lát cắt mỏng |  | ■ | ■ |  |  |  |  |  |  |  |
| M2 · Luồng chạy thông |  |  | ◆ |  |  |  |  |  |  |  |
| GĐ2 · Nhánh batch |  |  | ■ | ■ | ■ |  |  |  |  |  |
| M3 · Lambda đủ |  |  |  |  | ◆ |  |  |  |  |  |
| GĐ3 · Mở rộng + ML |  |  |  | ■ | ■ | ■ | ■ | ■ |  |  |
| M4 · Đóng băng tính năng |  |  |  |  |  |  |  | ◆ |  |  |
| GĐ4 · Vận hành, báo cáo |  |  |  |  |  |  |  | ■ | ■ | ■ |
| M5 · Sẵn sàng thuyết trình |  |  |  |  |  |  |  |  |  | ◆ |

■ = giai đoạn đang chạy · ◆ = mốc, tính vào cuối tuần đó. Các giai đoạn gối lên nhau: hạ tầng k3s được dựng song song với việc viết code trên Compose, và mở rộng nguồn bắt đầu trước khi nhánh batch xong hẳn.

Trạng thái mỗi việc là một trong ba giá trị: **Chưa bắt đầu**, **Đang làm**, **Xong**. Một việc chỉ chuyển sang **Xong** khi Thái đã review.

### M1 · Cuối tuần 1 · Thiết kế chốt, môi trường dev chạy

| # | Việc | Phụ trách | Trạng thái |
| --- | --- | --- | --- |
| M1.1 | Repo GitHub, cấu trúc thư mục, quy trình PR | Thái | Xong |
| M1.2 | So sánh HDFS và MinIO cho cụm k3s của nhóm, đề xuất một lựa chọn để Thái chốt | Đạt | Xong |
| M1.3 | Khảo sát máy của nhóm (số máy, RAM mỗi máy), chọn máy chạy cụm k3s | Thạch | Xong |
| M1.4 | docker-compose chạy Kafka, Spark, MongoDB và Elasticsearch trên máy mỗi người | Thái | Xong |
| M1.5 | Chốt danh sách trang báo và box VOZ; crawl thử 100 bản ghi mỗi nguồn; đo bình luận thường đến muộn bao lâu | Thái | Đang làm |
| M1.6 | Đề xuất cách demo: chạy real-time trực tiếp hay replay dữ liệu đã thu | Quang | Xong |
| M1.7 | Chốt ADR | Thái | Xong |

### M2 · Cuối tuần 3 · Luồng chạy thông trên k3s

| # | Việc | Phụ trách | Trạng thái |
| --- | --- | --- | --- |
| M2.1 | Cụm k3s đã cài Strimzi, Spark Operator, ECK | Thạch | Chưa bắt đầu |
| M2.2 | Crawler RSS một trang báo gửi vào Kafka | Thắng | Chưa bắt đầu |
| M2.3 | speed-sentiment bản đầu (sentiment tạm bằng luật) ghi vào Elasticsearch | Quang | Đang làm |
| M2.4 | 1 dashboard Kibana hiển thị kết quả real-time | Quang | Chưa bắt đầu |
| M2.5 | Image build tự động qua GitHub Actions | Thạch | Chưa bắt đầu |

### M3 · Cuối tuần 5 · Lambda đủ với một nguồn

| # | Việc | Phụ trách | Trạng thái |
| --- | --- | --- | --- |
| M3.1 | HDFS và Hive Metastore chạy trên k3s | Đạt | Chưa bắt đầu |
| M3.2 | raw-ingest ghi xuống HDFS | Quang | Chưa bắt đầu |
| M3.3 | batch-clean và batch-publish chạy hằng đêm | Đạt | Chưa bắt đầu |
| M3.4 | MongoDB chạy trên k3s; API gộp batch view và real-time view theo mốc T | Quang | Chưa bắt đầu |
| M3.5 | Backfill lịch sử bắt đầu chạy | Thắng | Chưa bắt đầu |
| M3.6 | Grafana có dashboard cơ bản cho Spark và Kafka | Thạch | Chưa bắt đầu |

### M4 · Cuối tuần 8 · Đóng băng tính năng

| # | Việc | Phụ trách | Trạng thái |
| --- | --- | --- | --- |
| M4.1 | Đủ 3 nguồn bắt buộc: thêm bình luận báo và VOZ | Thắng | Chưa bắt đầu |
| M4.2 | PhoBERT thật (ONNX) ở cả speed layer và batch layer | Thái | Chưa bắt đầu |
| M4.3 | Gắn tag, chủ đề, thực thể; reference-refresh chạy hằng tuần | Thái | Chưa bắt đầu |
| M4.4 | batch-analytics: join, chỉ số ngày, khoảng cách báo chí và dư luận | Đạt | Chưa bắt đầu |
| M4.5 | LDA và đồ thị thực thể bằng GraphFrames | Thái | Chưa bắt đầu |
| M4.6 | Dashboard Kibana và Streamlit đầy đủ | Quang | Chưa bắt đầu |
| M4.7 | Rà đủ 6 nhóm yêu cầu Spark của guideline | Thái | Chưa bắt đầu |
| M4.8 | (Tùy chọn) Crawler bình luận YouTube, chỉ làm khi M4.1–M4.7 đã xong | Thắng | Chưa bắt đầu |

### M5 · Cuối tuần 10 · Sẵn sàng thuyết trình

| # | Việc | Phụ trách | Trạng thái |
| --- | --- | --- | --- |
| M5.1 | Kiểm thử chịu lỗi: kill pod, kiểm không mất bản ghi | Quang | Chưa bắt đầu |
| M5.2 | Đo hiệu năng trước và sau tối ưu | Đạt | Chưa bắt đầu |
| M5.3 | Đối soát batch view với real-time view | Thái | Chưa bắt đầu |
| M5.4 | Báo cáo 4 phần, Lessons Learned theo đúng mẫu guideline | Cả nhóm, Thái tổng hợp | Chưa bắt đầu |
| M5.5 | Slide và video demo dự phòng | Thạch | Chưa bắt đầu |
| M5.6 | Tập demo 2 lần trên cụm thật | Cả nhóm | Chưa bắt đầu |
| M5.7 | Script replay và file Parquet có edge case cho phần 2 của demo | Quang | Chưa bắt đầu |

## 11. Theo dõi hằng tuần

Cuối mỗi tuần, thêm một mục mới lên trên cùng của phần này: ghi việc đã làm, người phụ trách, link PR, và câu hỏi cho tuần tới. Mỗi câu hỏi phải gắn với một việc trong bảng mốc (thêm việc mới nếu chưa có), để luôn có người được giao trả lời. Câu hỏi chưa xong thì chép sang tuần sau. Đồng thời cập nhật cột Trạng thái ở các bảng mốc phía trên.

### Tuần 1 · 12/10–18/10/2026

**Đã làm trong tuần**

| Việc | Phụ trách | Trạng thái | PR / ghi chú |
| --- | --- | --- | --- |
| M1.1 · Repo, cấu trúc thư mục, quy trình PR | Thái | Đang làm | đã có cấu trúc, CONTRIBUTING.md, mẫu PR, CI; còn commit, mở PR và bật bảo vệ nhánh main trên GitHub |
| M1.2 · Chọn lưu trữ dài hạn | Đạt, Thái chốt | Xong | giữ HDFS; MinIO bản community đã ngừng bảo trì |
| M1.3 · Khảo sát máy | Thạch | Đang làm | bảng máy ở mục 5 còn trống |
| M1.4 · docker-compose cho dev | Thái | Đang làm | chuyển từ Thạch sang Thái; đã có docker-compose.yml và smoke test, chờ cài Docker để chạy thử |
| M1.5 · Nguồn crawl, crawl thử | Thắng (Thái làm thay) | Đang làm | chờ Thái review; kết quả ở docs/m1.5-nguon-du-lieu.md: VnExpress, Tuổi Trẻ, VOZ box Điểm báo; bình luận đến muộn trung vị 3 giờ, p99 khoảng 2 ngày |
| M1.6 · Kịch bản demo | Quang | Xong | xem mục 7; phần 2 đổi thành thử edge case với khối lượng lớn |
| M1.7 · Chốt ADR | Thái | Xong | thêm MongoDB, Elasticsearch chỉ để tìm kiếm; các ADR còn lại giữ nguyên |

**Câu hỏi cho tuần tới**

- [x] Nhóm có bao nhiêu máy, mỗi máy bao nhiêu RAM, máy nào chạy cụm k3s? → M1.3 (Thạch): cả 5 máy, máy Thạch chạy k3s server; xem mục 5
- [x] Crawl những trang báo và box VOZ nào; bình luận thường đến muộn bao lâu? → M1.5 (Thắng): xem docs/m1.5-nguon-du-lieu.md
- [x] Nâng watermark của speed-sentiment từ 2 lên 6 giờ không? Bình luận Tuổi Trẻ chờ duyệt, 5% chờ hơn 2 giờ → M2.3 (Quang): nâng lên 6 giờ theo số đo 24/10 (16,7% bình luận Tuổi Trẻ trễ quá 2 giờ), xem mục 4 phần Speed layer
- [x] VnExpress chặn bot sau lượt crawl thử: lệnh chặn có tự gỡ không, crawler thật giãn cách bao nhiêu? → M2.2 (Thắng): đã tự gỡ trước 24/10; giãn cách 3–5 giây mỗi host, xem docs/m1.5-nguon-du-lieu.md
- [ ] Mục tiêu vài triệu bản ghi cần backfill bao nhiêu bài cũ và bắt đầu từ khi nào? → M3.5 (Thắng)
- [ ] Bản trên k3s dùng đúng phiên bản như docker-compose (Kafka 4.0, Spark 3.5.3, MongoDB 7.0, Elasticsearch 8.15) chưa? → M2.1 (Thạch)
- [x] Bản ghi hỏng đi thẳng vào Kafka, không qua crawler, thì job nào tách vào dlq.raw? → M2.3 (Quang): query A của speed-sentiment, xem mục 4 phần Speed layer
- [ ] Dữ liệu replay của demo cũng chảy xuống /raw qua raw-ingest; đánh dấu và loại khỏi master dataset bằng cách nào? → M5.7 (Quang)

### Tuần 0 · Chuẩn bị (đến 11/10/2026)

**Đã làm trong tuần**

| Việc | Phụ trách | Trạng thái | PR / ghi chú |
| --- | --- | --- | --- |
| Chọn đề tài: phân tích cảm xúc tin tức, bình luận và mạng xã hội tiếng Việt | Thái | Xong |  |
| Đặt tên project VietPulse | Thái | Xong |  |
| Đề xuất input, pipeline, output; chọn Lambda và k3s | Thái | Xong |  |
| Viết design doc và kế hoạch (file này) | Thái | Xong |   |
| Chốt phạm vi: 3 nguồn bắt buộc, bình luận YouTube là tùy chọn | Thái | Xong |  |

**Câu hỏi cho tuần tới**

- [x] Dùng HDFS thật hay MinIO? → M1.2 (Đạt đề xuất, Thái chốt): giữ HDFS
- [ ] Nhóm có bao nhiêu máy, mỗi máy bao nhiêu RAM, máy nào chạy cụm k3s? → M1.3 (Thạch): chép sang tuần 1
- [ ] Crawl những trang báo và box VOZ nào? → M1.5 (Thắng): chép sang tuần 1
- [x] Buổi demo chạy real-time trực tiếp hay replay dữ liệu đã thu? → M1.6 (Quang): xem mục 7

<details>
<summary>Mẫu cho tuần mới</summary>

```markdown
### Tuần N · dd/mm–dd/mm

**Đã làm trong tuần**

| Việc | Phụ trách | Trạng thái | PR / ghi chú |
| --- | --- | --- | --- |
|  |  |  |  |

**Câu hỏi cho tuần tới**

- [ ] Câu hỏi → mã việc trong bảng mốc (người phụ trách)
```

</details>
