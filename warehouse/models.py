from dataclasses import dataclass

@dataclass
class Table:
    name: str
    description: str


WAREHOUSE_TABLES = {
    "fact_trips": Table(
        name="fact_trips",
        description="Main trip fact table"
    ),
    "dim_vendor": Table(
        name="dim_vendor",
        description="Vendor dimension"
    ),
    "dim_date": Table(
        name="dim_date",
        description="Date dimension"
    ),
}