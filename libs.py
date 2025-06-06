import os
import re
import pandas as pd
import datetime
import config
from PyQt6.QtWidgets import (
    QWidget, QFileDialog, QMessageBox, QDialog,
    QListWidget, QPushButton, QVBoxLayout, QLabel
)
from PyQt6.QtCore import Qt
from sqlalchemy import inspect
from dateutil import parser



# ======== Import chung cho nhiều loại dữ liệu ========

def on_import_clicked(window, cfg):
    """Xử lý khi nhấn nút import, gọi hàm import file chung."""
    start_row = cfg.get("start_row", window.config.default_start_row)
    num_cols = cfg.get("num_cols", window.config.default_num_cols)
    required_cols = cfg.get("required_cols", window.config.default_required_cols)
    import_all_sheets = cfg.get("import_all_sheets", window.config.default_import_all_sheets)
    validate_and_format_func = cfg["validate_func"]
    table_name = cfg.get("table_name")

    import_file_general(
        window,
        num_cols=num_cols,
        required_cols=required_cols,
        start_row=start_row,
        import_all_sheets=import_all_sheets,
        validate_and_format_func=validate_and_format_func,
        table_name=table_name
    )

def import_file_general(self, num_cols, required_cols, start_row, import_all_sheets, validate_and_format_func, table_name=None):
    """Chọn nhiều file để import, gọi hàm import từng file."""
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
                file_path=file_path,
                num_cols=num_cols,
                required_cols=required_cols,
                start_row=start_row,
                import_all_sheets=import_all_sheets,
                validate_and_format_func=validate_and_format_func,
                table_name=table_name
            )
        except Exception as e:
            all_success = False
            QMessageBox.critical(self, "Lỗi Import", f"File: {file_path}\n{str(e)}")

    if all_success:
        QMessageBox.information(self, "Hoàn tất", "Tất cả file đã được import thành công.")

def import_file_configurable(self, file_path, num_cols, required_cols, start_row, import_all_sheets, validate_and_format_func, table_name=None):
    """Import file Excel hoặc CSV với các tham số cấu hình."""
    ext = os.path.splitext(file_path)[-1].lower()
    if ext in ['.xls', '.xlsx']:
        xls = pd.ExcelFile(file_path)
        sheets = xls.sheet_names
        selected_sheets = sheets if import_all_sheets else [sheets[0]]
        for sheet in selected_sheets:
            df = load_file_to_dataframe_with_error_handling(self, file_path, num_cols, required_cols, start_row, sheet)
            if df is not None:
                final_table_name = table_name if table_name else sanitize_table_name(sheet)
                process_and_import_dataframe(self, df, final_table_name, validate_and_format_func)
    elif ext == '.csv':
        df = load_file_to_dataframe_with_error_handling(self, file_path, num_cols, required_cols, start_row)
        if df is not None:
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            final_table_name = table_name if table_name else sanitize_table_name(base_name)
            process_and_import_dataframe(self, df, final_table_name, validate_and_format_func)
    else:
        raise Exception("Định dạng file không được hỗ trợ.")

def load_file_to_dataframe_with_error_handling(self, file_path, num_cols, required_cols, start_row, sheet_name=None):
    """Đọc file, định dạng dataframe, bắt lỗi và báo lỗi qua dialog."""
    try:
        df_raw = load_raw_file(file_path, sheet_name)
        return format_dataframe(df_raw, required_cols, start_row)
    except Exception as e:
        QMessageBox.critical(self, "Lỗi định dạng dữ liệu", f"Lỗi khi xử lý file {file_path}:\n{str(e)}")
        return None

def load_raw_file(file_path, sheet_name=None):
    """Đọc file Excel hoặc CSV thô."""
    ext = os.path.splitext(file_path)[-1].lower()
    if ext in ['.xls', '.xlsx']:
        return pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    elif ext == '.csv':
        return pd.read_csv(file_path, header=None)
    else:
        raise Exception("Định dạng file không được hỗ trợ.")

def format_dataframe(df, required_cols, start_row):
    """Định dạng dataframe: lấy header, check các cột bắt buộc."""
    if df.shape[0] < start_row:
        raise Exception(f"File không có đủ hàng để bắt đầu từ dòng {start_row}")

    header_row = df.iloc[start_row - 1, :].tolist()
    if not any(pd.notna(x) for x in header_row):
        raise Exception("Dòng tiêu đề (header) trống hoặc không hợp lệ")

    header_row_stripped = [str(col).strip() if pd.notna(col) else "" for col in header_row]

    df_data = df.iloc[start_row:, :]
    df_data.columns = header_row_stripped
    df_data = df_data.loc[:, [col for col in df_data.columns if col]]

    for col in required_cols:
        if col not in df_data.columns:
            raise Exception(f"Thiếu cột bắt buộc: {col}")

    return df_data[required_cols].reset_index(drop=True)

def process_and_import_dataframe(self, df, table_name, validate_and_format_func):
    """Xử lý dữ liệu theo hàm validate, convert ngày tháng, ghi vào DB."""
    processed_df = validate_and_format_func(df)
    if "OperationDate" in processed_df.columns:
        processed_df["OperationDate"] = pd.to_datetime(processed_df["OperationDate"], errors="raise")
    if not processed_df.empty:
        processed_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)

def process_and_import_dataframe(self, df, table_name, validate_and_format_func):
    """Xử lý dữ liệu theo hàm validate, convert ngày tháng, ghi vào DB."""
    processed_df = validate_and_format_func(df)
    # KHÔNG ép lại datetime khi lưu DB, vì đã là string dd-mmm-yy từ validate
    if not processed_df.empty:
        processed_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)



def sanitize_table_name(name):
    """Chuẩn hóa tên bảng SQL (thay ký tự không hợp lệ, thêm tiền tố nếu bắt đầu bằng số)."""
    sanitized = re.sub(r'\W+', '_', name.replace(' ', '_').lower())
    if sanitized and sanitized[0].isdigit():
        sanitized = f'table_{sanitized}'
    return sanitized

# ======== Hàm xử lý ngày tháng ========

def parse_1A_date(date_str):
    """Parse các định dạng ngày tháng đặc biệt cho dữ liệu 1A."""
    if isinstance(date_str, (pd.Timestamp, datetime.datetime, datetime.date)):
        return pd.to_datetime(date_str)

    date_str = str(date_str).strip()
    if not date_str or date_str.lower() == 'nan':
        raise ValueError("Chuỗi ngày rỗng hoặc 'nan'")

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

# ======== Xử lý dữ liệu đặc biệt cho 1A ========

Market_Report_imported = 0
Periods_imported = 0
Periods_1A = None
Market_Report_1A = None
Merged_1A = None
NOT_in_Market_Report = None
NOT_in_Periods = None

def validate_and_format_for_1Aperiods(df):
    global Periods_imported, Periods_1A
    Periods_1A = df
    Periods_imported = 1
    trigger_merge_if_ready()

def validate_and_format_for_1A_market_report(df):
    global Market_Report_imported, Market_Report_1A
    Market_Report_1A = df
    Market_Report_imported = 1
    trigger_merge_if_ready()

def trigger_merge_if_ready():
    if Periods_imported == 1 and Market_Report_imported == 1:
        merge_tables_for_1A()

def merge_tables_for_1A():
    global Periods_1A, Market_Report_1A, Merged_1A, NOT_in_Market_Report, NOT_in_Periods

    periods_cols = ['OL', 'FlightNbr', 'OperationDate', 'Frequency', 'DEP', 'ARR', 'ACV', 'SaleableCfg']
    market_cols = ['STD', 'C', 'Y', 'ACtype']

    periods_df = Periods_1A[periods_cols].copy()
    market_df = Market_Report_1A[['OperationDate', 'OL', 'FlightNbr', 'Frequency', 'DEP', 'ARR'] + market_cols].copy()

    merge_keys = ['OperationDate', 'OL', 'FlightNbr', 'Frequency', 'DEP', 'ARR']
    Merged_1A = pd.merge(periods_df, market_df, on=merge_keys, how='inner')

    not_in_market = pd.merge(market_df, periods_df, on=merge_keys, how='left', indicator=True)
    NOT_in_Market_Report = not_in_market[not_in_market['_merge'] == 'left_only'].drop(columns=['_merge'])

    not_in_periods = pd.merge(periods_df, market_df, on=merge_keys, how='left', indicator=True)
    NOT_in_Periods = not_in_periods[not_in_periods['_merge'] == 'left_only'].drop(columns=['_merge'])

# ======== Export Excel with table selection ========

def export_to_excel_dialog(parent: QWidget):
    inspector = inspect(parent.db_engine)
    table_names = inspector.get_table_names()
    if not table_names:
        QMessageBox.warning(
            parent,
            "Không có dữ liệu",
            "Không tìm thấy bảng dữ liệu trong cơ sở dữ liệu.",
            buttons=QMessageBox.StandardButton.Ok,
        )
        return

    dialog = QDialog(parent)
    dialog.setWindowTitle("Chọn bảng để xuất Excel")
    layout = QVBoxLayout(dialog)

    label = QLabel("Chọn bảng cần xuất Excel (có thể chọn nhiều):")
    layout.addWidget(label)

    list_widget = QListWidget()
    list_widget.addItems(table_names)
    list_widget.setSelectionMode(QListWidget.SelectionMode.MultiSelection)  # Cho phép chọn nhiều
    layout.addWidget(list_widget)

    btn_export = QPushButton("Xuất Excel")
    btn_export.clicked.connect(lambda: export_selected_table_to_excel(parent, dialog, list_widget))
    layout.addWidget(btn_export)

    dialog.setLayout(layout)
    dialog.exec()



def export_selected_table_to_excel(parent: QWidget, dialog: QDialog, list_widget: QListWidget):
    selected_items = list_widget.selectedItems()
    if not selected_items:
        QMessageBox.warning(parent, "Chưa chọn bảng", "Vui lòng chọn một hoặc nhiều bảng để xuất Excel.")
        return

    table_names = [item.text() for item in selected_items]

    save_path, _ = QFileDialog.getSaveFileName(
        parent, "Lưu file Excel", "exported_tables.xlsx", "Excel Files (*.xlsx)"
    )
    if not save_path:
        return

    try:
        with pd.ExcelWriter(save_path, engine='xlsxwriter', datetime_format='dd-mmm-yy', date_format='dd-mmm-yy') as writer:
            for table_name in table_names:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, parent.db_engine)
                # Nếu OperationDate là string dd-mmm-yy thì convert lại sang datetime trước khi export
                if 'OperationDate' in df.columns:
                    df['OperationDate'] = pd.to_datetime(df['OperationDate'], format='%d-%b-%y', errors='coerce')
                df.to_excel(writer, sheet_name=table_name[:31], index=False)
        tables_str = ", ".join(table_names)
        QMessageBox.information(
            parent,
            "Xuất thành công",
            f"Đã xuất các bảng sau ra file Excel:\n{tables_str}\n\nFile: {save_path}"
        )
    except Exception as e:
        QMessageBox.critical(parent, "Lỗi xuất Excel", f"Đã xảy ra lỗi khi xuất file:\n{str(e)}")

    dialog.accept()

def export_table_to_excel(parent: QWidget, table_name: str):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, parent.db_engine)
    save_path, _ = QFileDialog.getSaveFileName(
        parent, f"Lưu file Excel cho bảng {table_name}", f"{table_name}.xlsx", "Excel Files (*.xlsx)"
    )
    if save_path:
        # Nếu OperationDate là string dd-mmm-yy thì convert lại sang datetime
        if 'OperationDate' in df.columns:
            df['OperationDate'] = pd.to_datetime(df['OperationDate'], format='%d-%b-%y', errors='coerce')
        df.to_excel(save_path, index=False)
        QMessageBox.information(
            parent,
            "Xuất thành công",
            f"Đã xuất bảng {table_name} ra file Excel.",
            buttons=QMessageBox.StandardButton.Ok,
        )


def create_merged_1a(db_engine):
    Periods_1A = pd.read_sql("Periods_1A", db_engine)
    Market_Report_1A = pd.read_sql("Market_Report_1A", db_engine)
    join_cols = ["OL", "FlightNbr", "OperationDate", "Frequency", "DEP", "ARR"]

    merged = pd.merge(Market_Report_1A, Periods_1A, how='inner', on=join_cols)

    # Lấy export_cols từ config
    config = Config()
    export_cols = None
    for cfg in config.button_configs:
        if cfg.get("table_name") == "Market_Report_1A":
            export_cols = cfg.get("export_cols")
            break

    if export_cols is not None:
        merged = normalize_dataframe(merged, export_cols)

    not_in_market = Periods_1A[~Periods_1A.set_index(join_cols).index.isin(
        Market_Report_1A.set_index(join_cols).index)].reset_index(drop=True)
    not_in_periods = Market_Report_1A[~Market_Report_1A.set_index(join_cols).index.isin(
        Periods_1A.set_index(join_cols).index)].reset_index(drop=True)
    return merged, not_in_market, not_in_periods

def compare_skd_and_1a(skd_data, merged_1a):
    """
    So sánh dữ liệu SKD_Data với Merged_1A theo các trường hợp:
    - EQT/CON: so trường cấu hình
    - TIM: so trường thời gian
    - CNL: so các dòng có ở SKD_Data mà không có ở Merged_1A
    Trả về: compare_result, still_in_market_report
    """
    result_list = []
    still_in_market_report = None

    # 1. EQT hoặc CON: so trường cấu hình
    mask_eqt_con = skd_data['Change code'].isin(['EQT', 'CON'])
    if mask_eqt_con.any():
        fields_skd = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','ACV','SaleableCfg','C(S)','Y(S)','ACtype']
        fields_1a  = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','ACV','SaleableCfg','C','Y','ACtype']
        skd_eqt_con = skd_data[mask_eqt_con].copy()
        skd_eqt_con = skd_eqt_con.rename(columns={'C(S)': 'C', 'Y(S)': 'Y'})
        merged_eqt_con = merged_1a[fields_1a].copy()
        merged_eqt_con = merged_eqt_con.astype(str)
        skd_eqt_con = skd_eqt_con[fields_1a].astype(str)
        df_eqt_con = pd.merge(skd_eqt_con, merged_eqt_con, how='inner', on=fields_1a)
        if not df_eqt_con.empty:
            df_eqt_con['Change code'] = 'EQT/CON'
            result_list.append(df_eqt_con)

    # 2. TIM: so trường thời gian
    mask_tim = skd_data['Change code'] == 'TIM'
    if mask_tim.any():
        fields = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','STD','ACtype']
        skd_tim = skd_data[mask_tim][fields].astype(str)
        merged_tim = merged_1a[fields].astype(str)
        df_tim = pd.merge(skd_tim, merged_tim, how='inner', on=fields)
        if not df_tim.empty:
            df_tim['Change code'] = 'TIM'
            result_list.append(df_tim)

    # 3. CNL: so trường bay bị huỷ
    mask_cnl = skd_data['Change code'] == 'CNL'
    if mask_cnl.any():
        fields = ['OL','FlightNbr','OperationDate','Frequency']
        skd_cnl = skd_data[mask_cnl].copy()
        merged_cnl = merged_1a[fields].copy()
        # Tìm các dòng chỉ có ở SKD mà không có ở Merged_1A
        idx = ~skd_cnl.set_index(fields).index.isin(merged_cnl.set_index(fields).index)
        df_cnl = skd_cnl[idx][fields + ['DEP','ARR','Change code','Reason','SvType']]
        if not df_cnl.empty:
            result_list.append(df_cnl)
        # Nếu không còn chuyến nào bị huỷ, xuất các chuyến này vẫn còn trong Market_Report
        idx_still = ~merged_cnl.set_index(fields).index.isin(skd_cnl.set_index(fields).index)
        still_in_market_report = merged_cnl[idx_still]
    else:
        still_in_market_report = None

    # Gộp kết quả các so sánh (nếu có)
    if result_list:
        compare_result = pd.concat(result_list, ignore_index=True)
    else:
        compare_result = pd.DataFrame()

    if 'still_in_market_report' not in locals():
        still_in_market_report = None

    return compare_result, still_in_market_report


def normalize_dataframe(df, export_cols):
    """
    Chuẩn hóa DataFrame:
    - Chỉ giữ các cột theo export_cols (theo thứ tự).
    - Nếu thiếu cột nào thì thêm cột đó với giá trị trống.
    - Đảm bảo không có cột trùng tên.
    """
    # Bỏ cột trùng (giữ cột đầu tiên)
    seen = set()
    unique_cols = []
    for col in df.columns:
        if col not in seen:
            unique_cols.append(col)
            seen.add(col)
    df = df[unique_cols]

    # Thêm cột thiếu
    for col in export_cols:
        if col not in df.columns:
            df[col] = None

    # Đảm bảo thứ tự cột đúng
    df = df[export_cols]

    # Kiểm tra không còn cột trùng tên
    assert df.columns.is_unique, "DataFrame columns not unique!"

    return df