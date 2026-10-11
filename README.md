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

Kết quả của hai nhánh được gộp ở Elasticsearch và hiển thị trên Kibana và Streamlit. Toàn bộ hệ thống chạy trên Kubernetes (k3s).

**Công nghệ chính:** Kafka · Spark (PySpark) · HDFS · Elasticsearch · PhoBERT · k3s · Prometheus, Grafana

Kế hoạch chi tiết và tiến độ hằng tuần: [VietPulse-Plan-and-Tracker.md](VietPulse-Plan-and-Tracker.md)
