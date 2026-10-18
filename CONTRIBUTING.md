# Quy trình đóng góp

Tài liệu này mô tả cách cả nhóm làm việc trên repo. Kế hoạch và bảng việc nằm ở [VietPulse-Plan-and-Tracker.md](VietPulse-Plan-and-Tracker.md).

## Cài môi trường

Cần Python 3.11.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[crawler,dev]"
pytest
ruff check . && ruff format --check .
```

## Nhánh và commit

- Không ai push thẳng lên `main`. Mọi thay đổi đi qua pull request.
- Mỗi việc trong bảng mốc một nhánh, đặt tên `<loại>/<mã việc>-<mô tả ngắn>`, ví dụ `feat/m2.2-crawler-rss`, `docs/m1.5-nguon-du-lieu`.
- Loại: `feat` (tính năng), `fix` (sửa lỗi), `docs`, `test`, `chore` (cấu hình, CI, dọn dẹp).
- Commit message theo dạng `<loại>(<phạm vi>): <mô tả>`, ví dụ `feat(crawler): thêm crawler bình luận Tuổi Trẻ`.

## Pull request

1. Tiêu đề PR bắt đầu bằng mã việc: `[M2.2] Crawler RSS gửi vào Kafka`.
2. Điền đủ mẫu PR: làm gì, kiểm thử thế nào, có đổi hợp đồng dữ liệu không.
3. CI phải xanh (ruff và pytest).
4. Thái review mọi PR. PR do Thái viết thì một thành viên khác review.
5. Merge bằng **Squash and merge**, rồi xóa nhánh.
6. Sau khi merge, cập nhật cột Trạng thái của việc đó trong file kế hoạch.

## Đổi hợp đồng dữ liệu

Các file sau là hợp đồng dữ liệu (mục 3 của kế hoạch): `contracts/`, `common/envelope.py`, danh sách Kafka topic, cây thư mục HDFS, collection MongoDB và index Elasticsearch. PR chạm vào chúng phải được người phụ trách của **cả hai phía** đồng ý, rồi Thái review. Đổi cấu trúc payload thì tăng `schema_version`.

## Không commit

- Bí mật: mật khẩu, API key, file `.env`. Dùng Secret trên k3s.
- Dữ liệu crawl: thư mục `data/` đã nằm trong `.gitignore`.
- Tên hoặc email người bình luận dạng gốc: crawler chỉ lưu bản băm.

## Cài đặt trên GitHub (chủ repo làm một lần)

Vào **Settings** của repo:

- **Branches → Add rule** cho `main`: bật *Require a pull request before merging* (1 approval), *Require status checks to pass* (chọn job `test` của CI), tắt force push.
- **General → Pull Requests**: chỉ bật *Allow squash merging*, bật *Automatically delete head branches*.
