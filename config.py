# Sample aircraft data and Config class (you may paste your full version)
data_tables = {
    "Aircraft": {
        "columns": ["RegNr.", "ACV", "SaleableCfg", "C", "Z", "Y", "ACtype"],
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

TEXTS = {
    "main_window_title": "Ứng dụng chính",
    "missing_data_title": "Thiếu dữ liệu",
    "missing_table_msg": "Thiếu bảng: {tables}. Vui lòng import đủ dữ liệu.",
    "process_error_title": "Lỗi xử lý",
    "process_error_msg": "Lỗi khi xử lý {compare_type}: {error}",
    "save_excel_title": "Lưu kết quả dưới dạng Excel",
    "excel_file_filter": "Excel files (*.xlsx)",
    "save_done_title": "Hoàn tất",
    "save_done_msg": "Đã tạo bảng và lưu file: {file}",
    "save_error_title": "Lỗi ghi file",
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
            {
                "name": "SKD Data",
                "table_name": "SKD_Data",
                "required_cols": [
                    "FLT NBR", "Board Point", "Off Point", "From", "To", "DOW", "New ETD (LT)", "New ETA (LT)", "New CFG",
                    "TAIL #", "Change code", "Reason", "Service type"
                ],
                "col_map": {
                    "Board Point": "DEP",
                    "Off Point": "ARR",
                    "DOW": "Frequency",
                    "New ETD (LT)": "STD",
                    "New ETA (LT)": "STA",
                    "New CFG": "New CFG",
                    "TAIL #": "TAIL #",
                    "Change code": "Change code",
                    "Reason": "Reason",
                    "Service type": "SvType"
                },
                "export_cols": [
                    "OL", "FlightNbr", "OperationDate", "Frequency", "DEP", "ARR", "ACV", "SaleableCfg",
                    "STD", "C(S)", "Y(S)", "ACtype", "STA", "RegNr.", "C", "Y",
                    "Change code", "Reason", "SvType"
                ],
                "start_row": 1
            },
            {"name": "In", "table_name": "In_Data"},
            {"name": "So sánh AIMS và 1A", "table_name": "Compare_AIMS_1A_Data"},
            {"name": "So sánh SKD và 1A", "table_name": "Compare_SKD_1A_Data"},
            {"name": "Tìm kiếm", "table_name": "TimKiem_Data"},
            {"name": "Xuất dữ liệu DB", "table_name": "DB_Data"},
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
            elif cfg["name"] == "SKD Data":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"], dt=self.data_tables["Aircraft"]: \
                    validate_and_format_for_SKD(df, rc, cm, ec, dt)
            else:
                cfg["validate_func"] = lambda df: df

        self.texts = TEXTS

    def get_start_row(self, cfg):
        return cfg.get("start_row", self.default_start_row)
        
