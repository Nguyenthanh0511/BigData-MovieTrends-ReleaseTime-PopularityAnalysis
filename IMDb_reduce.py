#!/usr/bin/env python
import sys
from collections import defaultdict

# Khởi tạo dictionary lưu trữ dữ liệu theo năm
year_data = defaultdict(lambda: {
    "totalVotes": 0,
    "totalRating": 0.0,
    "count": 0
})

# Đọc dữ liệu đầu vào từ mapper
for line in sys.stdin:
    fields = line.strip().split("\t")

    if len(fields) < 3:
        continue

    start_year = fields[0]
    try:
        num_votes = int(fields[1])
        rating = float(fields[2])
    except ValueError:
        continue

    year_data[start_year]["totalVotes"] += num_votes
    year_data[start_year]["totalRating"] += rating
    year_data[start_year]["count"] += 1

# Xuất kết quả sau khi xử lý
for year, data in sorted(year_data.items()):
    if data["count"] == 0:
        avg_rating = 0
        avg_votes = 0
    else:
        avg_rating = data["totalRating"] / data["count"]
        avg_votes = data["totalVotes"] / data["count"]
    
    print(f"{year}\t{avg_rating:.2f}\t{avg_votes:.2f}")
