import os
import re
import pandas as pd
from PyQt6.QtWidgets import QFileDialog, QMessageBox
from sqlalchemy import inspect

def on_import_clicked(self, button_name):
    button_config = next((cfg for cfg in self.config.button_configs if cfg["name"] == button_name), None)

    if not button_config or "table_name" not in button_config:
        QMessageBox.critical(self, "Lỗi", "Không tìm thấy cấu hình cho nút này.")
        return

    self.num_cols = button_config.get("num_cols", self.num_cols)
    self.required_cols = button_config.get("required_cols", self.required_cols)
    self.start_row = button_config.get("start_row", self.start_row)
    self.import_all_sheets = button_config.get("import_all_sheets", self.import_all_sheets)
    validate_func = button_config.get("validate_func", lambda df: df)

    import_file_general(
        self,
        self.num_cols,
        self.required_cols,
        self.start_row,
        self.import_all_sheets,
        validate_func,
        table_name=button_config["table_name"]
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