import os
import re
import pandas as pd
from PyQt6.QtWidgets import QFileDialog, QMessageBox
from sqlalchemy import inspect
from dateutil import parser
import datetime

def on_import_clicked(window, cfg):
    start_row = cfg.get("start_row", window.config.default_start_row)
    num_cols = cfg.get("num_cols", window.config.default_num_cols)
    required_cols = cfg.get("required_cols", window.config.default_required_cols)
    import_all_sheets = cfg.get("import_all_sheets", window.config.default_import_all_sheets)
    validate_and_format_func = cfg["validate_func"]
    table_name = cfg.get("table_name")

    import_file_general(
        window,
        num_cols,
        required_cols,
        start_row,
        import_all_sheets,
        validate_and_format_func,
        table_name
    )


def import_file_general(self, num_cols, required_cols, start_row, import_all_sheets, validate_and_format_func, table_name=None):
    files, _ = QFileDialog.getOpenFileNames(
        self,
        "Chọn file Excel hoặc CSV",
        "",
        "Excel files (*.xls *.xlsx);;CSV Files (*.csv);;All Files (*)"
    )
    if not files:
        return

    all_success = True
    for file_path in files:
        try:
            import_file_configurable(
                self,
                file_path,
                num_cols,
                required_cols,
                start_row,
                import_all_sheets,
                validate_and_format_func,
                table_name=table_name
            )
        except Exception as e:
            all_success = False
            QMessageBox.critical(self, "Lỗi Import", f"File: {file_path}\n{str(e)}")

    if all_success:
        QMessageBox.information(self, "Hoàn tất", "Tất cả file đã được import thành công.")

def import_file_configurable(self, file_path, num_cols, required_cols, start_row, import_all_sheets, validate_and_format_func, table_name=None):
    ext = os.path.splitext(file_path)[-1].lower()
    if ext in ['.xls', '.xlsx']:
        xls = pd.ExcelFile(file_path)
        sheet_names = xls.sheet_names
        selected_sheets = sheet_names if import_all_sheets else [sheet_names[0]]
        for sheet in selected_sheets:
            df = load_file_to_dataframe(file_path, num_cols, required_cols, start_row, sheet_name=sheet)
            final_table_name = table_name if table_name else sanitize_table_name(sheet)
            process_and_import_dataframe(self, df, final_table_name, validate_and_format_func)
    elif ext == '.csv':
        df = load_file_to_dataframe(file_path, num_cols, required_cols, start_row)
        final_table_name = table_name if table_name else sanitize_table_name(os.path.splitext(os.path.basename(file_path))[0])
        process_and_import_dataframe(self, df, final_table_name, validate_and_format_func)
    else:
        raise Exception("Định dạng file không được hỗ trợ.")

def load_file_to_dataframe(file_path, num_cols, required_cols, start_row, sheet_name=None):
    import os
    import pandas as pd

    ext = os.path.splitext(file_path)[-1].lower()
    if ext in ['.xls', '.xlsx']:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    elif ext == '.csv':
        df = pd.read_csv(file_path, header=None)
    else:
        raise Exception("Định dạng file không được hỗ trợ.")

    header_row = df.iloc[start_row - 1, :].tolist()
    header_row_stripped = [str(col).strip() if pd.notna(col) else "" for col in header_row]

    df_data = df.iloc[start_row:, :]
    df_data.columns = header_row_stripped

    df_data = df_data.loc[:, [col for col in df_data.columns if col]]

    for col in required_cols:
        if col not in df_data.columns:
            raise Exception(f"Thiếu cột: {col}")

    df_data = df_data[required_cols].reset_index(drop=True)
    return df_data

def process_and_import_dataframe(self, df, table_name, validate_and_format_func):
    processed_df = validate_and_format_func(df)
    # Đảm bảo OperationDate đúng kiểu datetime64
    if "OperationDate" in processed_df.columns:
        processed_df["OperationDate"] = pd.to_datetime(processed_df["OperationDate"], errors="raise")
    print(processed_df.dtypes)  # Debug: phải thấy datetime64[ns] cho OperationDate
    print(processed_df.head())  # Debug: phải thấy yyyy-mm-dd đúng
    if not processed_df.empty:
        processed_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)

def sanitize_table_name(name):
    sanitized = re.sub(r'\W+', '_', name.replace(' ', '_').lower())
    if sanitized and sanitized[0].isdigit():
        sanitized = f'table_{sanitized}'
    return sanitized

def parse_1A_date(date_str):
    if isinstance(date_str, (pd.Timestamp, datetime.datetime, datetime.date)):
        # Nếu là datetime, trả về đúng luôn
        return pd.to_datetime(date_str)
    date_str = str(date_str).strip()
    if not date_str or date_str.lower() == 'nan':
        raise ValueError("Chuỗi ngày rỗng hoặc 'nan'")
    # Normalize tháng
    date_str = re.sub(
        r"(\d{2})([\/\-])([A-Z]{3})([\/\-])(\d{2,4})",
        lambda m: f"{m.group(1)}{m.group(2)}{m.group(3).title()}{m.group(4)}{m.group(5)}",
        date_str.upper()
    )
    date_str = re.sub(
        r"(\d{2})([A-Z]{3})(\d{2,4})",
        lambda m: f"{m.group(1)}{m.group(2).title()}{m.group(3)}",
        date_str.upper()
    )
    for fmt in ("%d%b%y", "%d%b%Y", "%d-%b-%y", "%d-%b-%Y", "%d/%b/%y", "%d/%b/%Y",
                "%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d"):
        try:
            return pd.to_datetime(date_str, format=fmt, errors="raise", dayfirst=True)
        except Exception:
            continue
    try:
        return parser.parse(date_str, dayfirst=True, fuzzy=True)
    except Exception:
        raise ValueError(f"Sai định dạng ngày: '{date_str}'")
        
        
# --- Biến trạng thái và DataFrame toàn cục cho luồng 1A ---
Market_Report_imported = 0
Periods_imported = 0
Periods_1A = None
Market_Report_1A = None

def validate_and_format_for_1Aperiods(df):
    """
    Hàm này sẽ được dùng làm validate_func khi import bảng Periods_1A.
    Sau khi validate thành công sẽ cập nhật trạng thái và trigger merge nếu cần.
    """
    global Periods_imported, Periods_1A
    # TODO: Validate và format df nếu cần
    Periods_1A = df
    Periods_imported = 1
    trigger_merge_if_ready()

def validate_and_format_for_1A_market_report(df):
    """
    Hàm này sẽ được dùng làm validate_func khi import bảng Market_Report_1A.
    Sau khi validate thành công sẽ cập nhật trạng thái và trigger merge nếu cần.
    """
    global Market_Report_imported, Market_Report_1A
    # TODO: Validate và format df nếu cần
    Market_Report_1A = df
    Market_Report_imported = 1
    trigger_merge_if_ready()

def trigger_merge_if_ready():
    if Periods_imported == 1 and Market_Report_imported == 1:
        merge_tables_for_1A()

def merge_tables_for_1A():
    """
    Sinh bảng Merged_1A, NOT_in_Market_Report, NOT_in_Periods theo yêu cầu.
    """
    global Periods_1A, Market_Report_1A

    periods_cols = ['OL', 'FlightNbr', 'OperationDate', 'Frequency', 'DEP', 'ARR', 'ACV', 'SaleableCfg']
    market_cols = ['STD', 'C', 'Y', 'ACtype']

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