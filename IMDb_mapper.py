#!/usr/bin/env python
import sys

# Đọc dữ liệu từ stdin
for line in sys.stdin:
    fields = line.strip().split("\t")
    
    # Kiểm tra định dạng
    if len(fields) < 4:
        continue
    
    tconst = fields[0]       # Mã phim
    start_year = fields[1]   # Năm phát hành
    num_votes = fields[2]    # Tổng số lượt đánh giá
    rating = fields[3]       # Điểm đánh giá trung bình

    # Kiểm tra giá trị hợp lệ
    if start_year.isdigit():
        print(f"{start_year}\t{num_votes}\t{rating}")
