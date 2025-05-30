import os
import re
import pandas as pd
from PyQt6.QtWidgets import QFileDialog, QMessageBox
from sqlalchemy import inspect

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
    if not processed_df.empty:
        processed_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)

def sanitize_table_name(name):
    sanitized = re.sub(r'\W+', '_', name.replace(' ', '_').lower())
    if sanitized and sanitized[0].isdigit():
        sanitized = f'table_{sanitized}'
    return sanitized

def export_to_excel_dialog(self):
    file_path, _ = QFileDialog.getSaveFileName(
        self,
        "Chọn nơi lưu file Excel",
        "",
        "Excel files (*.xlsx)"
    )
    if not file_path:
        return

    if not file_path.lower().endswith(".xlsx"):
        file_path += ".xlsx"

    try:
        inspector = inspect(self.db_engine)
        table_names = inspector.get_table_names()
        if not table_names:
            QMessageBox.warning(self, "Không có dữ liệu", "Không có bảng dữ liệu nào để xuất ra.")
            return

        with pd.ExcelWriter(file_path, engine='xlsxwriter', datetime_format='dd-mmm-yy', date_format='dd-mmm-yy') as writer:
            for table in table_names:
                df = pd.read_sql_table(table, self.db_engine)
                if 'OperationDate' in df.columns:
                    df['OperationDate'] = pd.to_datetime(df['OperationDate'], errors='coerce')
                df.to_excel(writer, sheet_name=table[:31], index=False)
                worksheet = writer.sheets[table[:31]]
                if 'OperationDate' in df.columns:
                    col_idx = df.columns.get_loc('OperationDate')
                    worksheet.set_column(col_idx, col_idx, 15)
        QMessageBox.information(self, "Xuất Excel", "Dữ liệu đã được xuất ra Excel thành công!")
    except Exception as e:
        QMessageBox.critical(self, "Lỗi Xuất Excel", str(e))
        
        
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