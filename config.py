from functions import validate_and_format_for_1Aperiods, validate_and_format_for_1A_market_report

class Config:
    def __init__(self):
        self.button_configs = [
            {
                "name": "1A Periods",
                "table_name": "Periods_1A",
                "num_cols": 10,
                "required_cols": [
                    'Type', 'Flight', 'Start', 'End', 'Frequency',
                    'I/D', 'Route', 'Svc', 'Config Code', 'Codeshare'
                ],
                "start_row": 1,
                "import_all_sheets": True,
                "validate_func": lambda df: validate_and_format_for_1Aperiods(
                    df, [
                        'Type', 'Flight', 'Start', 'End', 'Frequency',
                        'I/D', 'Route', 'Svc', 'Config Code', 'Codeshare'
                    ]
                )
            },
            {
                "name": "1A Market Report",
                "table_name": "Market_Report_1A",
                "num_cols": 10,
                "required_cols": [
                    "Flt Dt", "Al", "Flt", "Day", "Dep", "Brd",
                    "Off", "CAP(C)", "CAP(Y)", "Eqp"
                ],
                "start_row": 1,
                "import_all_sheets": True,
                "validate_func": validate_and_format_for_1A_market_report
            },
            {
                "name": "Xóa", "table_name": "Xoa_Data"
            },
            {
                "name": "Cài đặt", "table_name": "CaiDat_Data"
            },
            {
                "name": "Giúp đỡ", "table_name": "Help_Data"
            },
            {
                "name": "In", "table_name": "In_Data"
            },
            {
                "name": "Tải lên", "table_name": "TaiLen_Data"
            },
            {
                "name": "Tìm kiếm", "table_name": "TimKiem_Data"
            },
            {
                "name": "Đồng bộ", "table_name": "DongBo_Data"
            },
            {
                "name": "Thoát", "table_name": None
            }
        ]

        self.background_images = [
            "bg_1.jpg", "bg_2.jpg", "bg_3.jpg", "bg_4.jpg", "bg_5.jpg",
            "bg_6.jpg", "bg_7.jpg", "bg_8.jpg", "bg_9.jpg", "bg_10.jpg"
        ]

        # fallback values
        self.default_num_cols = 10
        self.default_required_cols = []
        self.default_start_row = 2
        self.default_import_all_sheets = True


data_tables = {
    "Aircraft": {
        "columns": ["RegNr.", "ACV", "SaleableCfg", "C", "Z", "Y", "ACType"],
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