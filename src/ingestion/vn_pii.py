"""Generate realistic (but fake) Vietnamese PII.

Faker's vi_VN locale produces odd output (e.g. "Quý ông ...", English street
names), so we use small hand-made lists instead. A seeded random.Random makes
the output deterministic: same seed → same people on every run.
"""

import random
import unicodedata

LAST_NAMES = [
    "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Huỳnh", "Phan", "Vũ", "Võ", "Đặng",
    "Bùi", "Đỗ", "Hồ", "Ngô", "Dương", "Lý",
]
MIDDLE_NAMES = {
    "M": ["Văn", "Hữu", "Đức", "Minh", "Quang", "Thành", "Công", "Gia", "Hoàng", "Anh"],
    "F": ["Thị", "Ngọc", "Thu", "Thanh", "Minh", "Phương", "Mai", "Kim", "Bảo", "Hồng"],
}
FIRST_NAMES = {
    "M": ["An", "Bình", "Cường", "Dũng", "Duy", "Hải", "Hiếu", "Hùng", "Huy", "Khang",
          "Khoa", "Long", "Nam", "Phúc", "Quân", "Sơn", "Tài", "Thắng", "Trung", "Tuấn"],
    "F": ["Anh", "Chi", "Dung", "Giang", "Hà", "Hạnh", "Hoa", "Hương", "Lan", "Linh",
          "Mai", "My", "Ngân", "Nhung", "Oanh", "Phương", "Quỳnh", "Thảo", "Trang", "Vy"],
}
STREETS = [
    "Lê Lợi", "Nguyễn Huệ", "Trần Hưng Đạo", "Hai Bà Trưng", "Lý Thường Kiệt",
    "Điện Biên Phủ", "Nguyễn Trãi", "Cách Mạng Tháng Tám", "Láng Hạ", "Phan Chu Trinh",
]
CITIES = [
    ("Hà Nội", ["Ba Đình", "Hoàn Kiếm", "Đống Đa", "Cầu Giấy", "Hai Bà Trưng"]),
    ("TP. Hồ Chí Minh", ["Quận 1", "Quận 3", "Bình Thạnh", "Phú Nhuận", "Tân Bình"]),
    ("Đà Nẵng", ["Hải Châu", "Thanh Khê", "Sơn Trà"]),
    ("Hải Phòng", ["Hồng Bàng", "Lê Chân", "Ngô Quyền"]),
    ("Cần Thơ", ["Ninh Kiều", "Cái Răng"]),
]
MOBILE_PREFIXES = ["032", "033", "034", "035", "036", "037", "038", "039",
                   "070", "076", "077", "078", "079", "081", "083", "084",
                   "085", "086", "088", "089", "090", "091", "093", "094", "096", "097", "098"]


def strip_accents(text: str) -> str:
    """'Nguyễn Văn Đức' → 'Nguyen Van Duc' (for email addresses)."""
    text = text.replace("đ", "d").replace("Đ", "D")
    normalized = unicodedata.normalize("NFD", text)
    return "".join(c for c in normalized if unicodedata.category(c) != "Mn")


class VietnamesePII:
    def __init__(self, seed: int):
        self.rng = random.Random(seed)

    def full_name(self, gender: str) -> str:
        return " ".join(
            [
                self.rng.choice(LAST_NAMES),
                self.rng.choice(MIDDLE_NAMES[gender]),
                self.rng.choice(FIRST_NAMES[gender]),
            ]
        )

    def phone(self) -> str:
        return self.rng.choice(MOBILE_PREFIXES) + f"{self.rng.randrange(10**7):07d}"

    def email(self, full_name: str, customer_id: int) -> str:
        parts = strip_accents(full_name).lower().split()
        # e.g. "Nguyen Van Duc" → "ducnv1234@gmail.com"; customer_id keeps it unique
        local = parts[-1] + "".join(p[0] for p in parts[:-1]) + str(customer_id)
        return f"{local}@{self.rng.choice(['gmail.com', 'yahoo.com', 'outlook.com'])}"

    def address(self) -> str:
        city, districts = self.rng.choice(CITIES)
        return (
            f"{self.rng.randint(1, 300)} {self.rng.choice(STREETS)}, "
            f"{self.rng.choice(districts)}, {city}"
        )
