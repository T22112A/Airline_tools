from functions import (
    validate_and_format_for_1Aperiods,
    validate_and_format_for_1A_market_report,
    validate_and_format_for_AIMS
)

data_tables = {
    "Aircraft": {
        "columns": ["RegNr.", "ACV", "SaleableCfg", "C", "Z", "Y", "EquipmentType"],
        "rows": [
            ["PK-BBK", "BBK", "BBK", 0, 0, 215, 737],
            ["JU-1410", "JU1", "JUA", 0, 0, 180, 320],
            ["VN-A227", "227", "27A", 8, 0, 176, 321],
            ["VN-A585", "585", "85A", 8, 0, 184, 321],
            ["VN-A594", "585", "85A", 8, 0, 184, 321],
            ["VN-A596", "596", "96A", 8, 0, 168, 320],
            ["VN-A597", "585", "85A", 8, 0, 184, 321]
        ]
    }
}

class Config:
    def __init__(self):
        self.data_tables = data_tables
        self.default_num_cols = 10
        self.default_required_cols = []
        self.default_start_row = 2
        self.default_import_all_sheets = True
        self.background_images = [
            "bg_1.jpg", "bg_2.jpg", "bg_3.jpg", "bg_4.jpg", "bg_5.jpg",
            "bg_6.jpg", "bg_7.jpg", "bg_8.jpg", "bg_9.jpg", "bg_10.jpg"
        ]

        self.button_configs = [
            {
                "name": "1A Periods",
                "table_name": "Periods_1A",
                "required_cols": [
                    'Type', 'Flight', 'Start', 'End', 'Frequency',
                    'I/D', 'Route', 'Svc', 'Config Code', 'Codeshare'
                ],
                "col_map": {
                    "Type": "Type",
                    "Flight": "Flight",
                    "Start": "Start",
                    "End": "End",
                    "Frequency": "Frequency",
                    "I/D": "I/D",
                    "Route": "Route",
                    "Svc": "Svc",
                    "Config Code": "Config Code",
                    "Codeshare": "Codeshare"
                },
                "export_cols": [
                    'Type', 'OL', 'FlightNbr', 'OperationDate', 'Frequency', 'I/D',
                    'DEP', 'ARR', 'Svc', 'ACV', 'SaleableCfg', 'Codeshare'
                ],
                "start_row": 1
            },
            {
                "name": "1A Market Report",
                "table_name": "Market_Report_1A",
                "required_cols": [
                    "Flt Dt", "Al", "Flt", "Day", "Dep", "Brd",
                    "Off", "CAP(C)", "CAP(Y)", "Eqp"
                ],
                "col_map": {
                    "Flt Dt": "Flt Dt",
                    "Al": "Al",
                    "Flt": "Flt",
                    "Day": "Day",
                    "Dep": "Dep",
                    "Brd": "Brd",
                    "Off": "Off",
                    "CAP(C)": "CAP(C)",
                    "CAP(Y)": "CAP(Y)",
                    "Eqp": "Eqp"
                },
                "export_cols": [
                    "OperationDate", "OL", "FlightNbr", "Frequency", "STD",
                    "DEP", "ARR", "C", "Y", "ACtype"
                ],
                "start_row": 1
            },
            {
                "name": "AIMS Data",
                "table_name": "AIMS_Data",
                "required_cols": [
                    "DATE", "FLT", "TYPE", "REG", "AC", "DEP", "ARR", "STD", "STA", "BLOCK", "FLThr", "AC CONFIG"
                ],
                "col_map": {
                    "DATE": "OperationDate",
                    "FLT": "FlightNbr",
                    "TYPE": "Type",
                    "REG": "RegNr.",
                    "AC": "ACtype",
                    "DEP": "DEP",
                    "ARR": "ARR",
                    "STD": "STD",
                },
                "export_cols": [
                    "OperationDate", "FlightNbr", "Type", "RegNr.", "ACtype",
                    "DEP", "ARR", "STD", "C", "Y", "ACV", "SaleableCfg"
                ],
                "start_row": 3  # Dòng 3 là header thực tế của file AIMS!
            },
            # Các nút placeholder cho đủ 10 nút
            {"name": "Cài đặt", "table_name": "CaiDat_Data"},
            {"name": "Giúp đỡ", "table_name": "Help_Data"},
            {"name": "In", "table_name": "In_Data"},
            {"name": "Tải lên", "table_name": "TaiLen_Data"},
            {"name": "Tìm kiếm", "table_name": "TimKiem_Data"},
            {"name": "Đồng bộ", "table_name": "DongBo_Data"},
            {"name": "Thoát", "table_name": None}
        ]

        # Gán validate_func cho từng loại
        for cfg in self.button_configs:
            if cfg["name"] == "1A Periods":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: \
                    validate_and_format_for_1Aperiods(df, rc, cm, ec)
            elif cfg["name"] == "1A Market Report":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: \
                    validate_and_format_for_1A_market_report(df, rc, cm, ec)
            elif cfg["name"] == "AIMS Data":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"], dt=self.data_tables["Aircraft"]: \
                    validate_and_format_for_AIMS(df, rc, cm, ec, dt)
            else:
                cfg["validate_func"] = lambda df: df

    def get_start_row(self, cfg):
        return cfg.get("start_row", self.default_start_row)