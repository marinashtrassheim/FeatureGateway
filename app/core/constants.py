from enum import Enum


class Brand(str, Enum):
    """Розничный бренд (префикс ключей pers_item / pers_user_item)."""

    LO = "lo"
    MNTK = "mntk"
    UTK = "utk"


class Channel(str, Enum):
    """Канал (для будущих моделей)."""

    DELIVERY = "delivery"
    PICKUP = "pickup"
    ALL = "all"
