from functions import (
    validate_and_format_for_1Aperiods,
    validate_and_format_for_1A_market_report,
    validate_and_format_for_AIMS
)
import pandas as pd

# Biến toàn cục cho trigger merge tự động
Market_Report_imported = 0
Periods_imported = 0
Periods_1A = None
Market_Report_1A = None

def set_periods_1A(df):
    global Periods_imported, Periods_1A
    Periods_1A = df
    Periods_imported = 1
    trigger_merge_if_ready()

def set_market_report_1A(df):
    global Market_Report_imported, Market_Report_1A
    Market_Report_1A = df
    Market_Report_imported = 1
    trigger_merge_if_ready()

def trigger_merge_if_ready():
    if Periods_imported == 1 and Market_Report_imported == 1:
        merge_tables_for_1A()

def merge_tables_for_1A():
    global Periods_1A, Market_Report_1A
    periods_cols = ['OL', 'FlightNbr', 'OperationDate', 'Frequency', 'DEP', 'ARR', 'ACV', 'SaleableCfg']
    market_cols = ['STD', 'C', 'Y', 'ACtype']
    if Periods_1A is None or Market_Report_1A is None:
        print("Chưa có đủ dữ liệu để merge.")
        return
    periods_df = Periods_1A[periods_cols].copy()
    market_df = Market_Report_1A[['OperationDate', 'OL', 'FlightNbr', 'Frequency', 'DEP', 'ARR'] + market_cols].copy()
    merge_keys = ['OperationDate', 'OL', 'FlightNbr', 'Frequency', 'DEP', 'ARR']
    merged = pd.merge(periods_df, market_df, on=merge_keys, how='inner')
    globals()['Merged_1A'] = merged

    not_in_market = pd.merge(market_df, periods_df, on=merge_keys, how='left', indicator=True)
    globals()['NOT_in_Market_Report'] = not_in_market[not_in_market['_merge'] == 'left_only'].drop(columns=['_merge'])

    not_in_periods = pd.merge(periods_df, market_df, on=merge_keys, how='left', indicator=True)
    globals()['NOT_in_Periods'] = not_in_periods[not_in_periods['_merge'] == 'left_only'].drop(columns=['_merge'])

    print("Merged_1A created with shape:", merged.shape)
    print("NOT_in_Market_Report created with shape:", globals()['NOT_in_Market_Report'].shape)
    print("NOT_in_Periods created with shape:", globals()['NOT_in_Periods'].shape)

# Dữ liệu mẫu máy bay
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

        # Gán validate_func cho từng loại, trigger merge nếu là bảng liên quan 1A
        for cfg in self.button_configs:
            if cfg["name"] == "1A Periods":
                # validate_func vừa validate vừa set trigger merge
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: (
                    lambda result: (set_periods_1A(result), result)[1]
                )(validate_and_format_for_1Aperiods(df, rc, cm, ec))
            elif cfg["name"] == "1A Market Report":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"]: (
                    lambda result: (set_market_report_1A(result), result)[1]
                )(validate_and_format_for_1A_market_report(df, rc, cm, ec))
            elif cfg["name"] == "AIMS Data":
                cfg["validate_func"] = lambda df, rc=cfg["required_cols"], cm=cfg["col_map"], ec=cfg["export_cols"], dt=self.data_tables["Aircraft"]: \
                    validate_and_format_for_AIMS(df, rc, cm, ec, dt)
            else:
                cfg["validate_func"] = lambda df: df

    def get_start_row(self, cfg):
        return cfg.get("start_row", self.default_start_row)