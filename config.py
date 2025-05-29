from functions import (
    validate_and_format_for_1Aperiods,
    validate_and_format_for_1A_market_report,
    validate_and_format_for_AIMS
)

class Config:
    def __init__(self):
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
                ]
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
                ]
            },
            {
                "name": "AIMS Data",
                "table_name": "AIMS_Data",
                "required_cols": [
                    "DATE", "FLT", "TYPE", "REG", "AC", "DEP", "ARR", "STD", "BLOCK", "FLThr", "AC CONFIG"
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
                    # BLOCK, FLThr, AC CONFIG giữ nguyên tên
                },
                "export_cols": [
                    "OperationDate", "FlightNbr", "Type", "RegNr.", "ACtype",
                    "DEP", "ARR", "STD", "C", "Y"
                ]
            },
            {"name": "Cài đặt", "table_name": "CaiDat_Data"},
            {"name": "Giúp đỡ", "table_name": "Help_Data"},
            {"name": "In", "table_name": "In_Data"},
            {"name": "Tải lên", "table_name": "TaiLen_Data"},
            {"name": "Tìm kiếm", "table_name": "TimKiem_Data"},
            {"name": "Đồng bộ", "table_name": "DongBo_Data"},
            {"name": "Thoát", "table_name": None}
        ]

        # Thiết lập validate_func cho từng loại
        for cfg in self.button_configs:
            if cfg["name"] == "1A Periods":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: \
                    validate_and_format_for_1Aperiods(df, rc, cm, ec)
            elif cfg["name"] == "1A Market Report":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: \
                    validate_and_format_for_1A_market_report(df, rc, cm, ec)
            elif cfg["name"] == "AIMS Data":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: \
                    validate_and_format_for_AIMS(df, rc, cm, ec)
            else:
                cfg["validate_func"] = lambda df: df

        # Các thuộc tính mặc định, hình nền...
        self.background_images = [
            "bg_1.jpg", "bg_2.jpg", "bg_3.jpg", "bg_4.jpg", "bg_5.jpg",
            "bg_6.jpg", "bg_7.jpg", "bg_8.jpg", "bg_9.jpg", "bg_10.jpg"
        ]
        self.default_num_cols = 10
        self.default_required_cols = []
        self.default_start_row = 2
        self.default_import_all_sheets = True