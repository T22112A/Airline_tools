import os
import re
import pandas as pd
import functions
import logger  # <--- Thêm dòng này
from PyQt6.QtWidgets import (
    QWidget, QFileDialog, QMessageBox, QDialog,
    QListWidget, QPushButton, QVBoxLayout, QLabel,
    QProgressDialog, QApplication
)
from PyQt6.QtCore import Qt
from sqlalchemy import inspect, text
from sqlalchemy.exc import OperationalError
from config import Config

def on_import_clicked(window, cfg):
    name = cfg.get("name", "")
    if name == "1A Periods":
        cfg["validate_func"] = lambda df: functions.validate_and_format_for_1Aperiods(
            df, cfg["required_cols"], cfg["col_map"], cfg["export_cols"]
        )
    elif name == "1A Market Report":
        cfg["validate_func"] = lambda df: functions.validate_and_format_for_1A_market_report(
            df, cfg["required_cols"], cfg["col_map"], cfg["export_cols"]
        )
    elif name == "AIMS Data":
        aircraft_table = window.config.data_tables["Aircraft"]
        cfg["validate_func"] = lambda df: functions.validate_and_format_for_AIMS(
            df, cfg["required_cols"], cfg["col_map"], cfg["export_cols"], aircraft_table
        )
    elif name == "SKD Data":
        aircraft_table = window.config.data_tables["Aircraft"]
        cfg["validate_func"] = lambda df: functions.validate_and_format_for_SKD(
            df, cfg["required_cols"], cfg["col_map"], cfg["export_cols"], aircraft_table
        )
    else:
        cfg["validate_func"] = lambda df: df  # fallback

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

    progress = QProgressDialog("Đang import các file...", "Hủy", 0, len(files), self)
    progress.setWindowTitle("Đang xử lý")
    progress.setWindowModality(Qt.WindowModality.ApplicationModal)
    progress.show()

    all_success = True
    for i, file_path in enumerate(files):
        progress.setValue(i)
        QApplication.processEvents()
        if progress.wasCanceled():
            break

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

    progress.setValue(len(files))

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
            final_table_name = table_name if table_name else sanitize_table_name(sheet)
            logger.log_info(f'Đang import sheet {sheet} từ file {file_path} vào bảng {final_table_name}')
            df = load_file_to_dataframe_with_error_handling(self, file_path, num_cols, required_cols, start_row, sheet)
            if df is not None:
                final_table_name = table_name if table_name else sanitize_table_name(sheet)
                process_and_import_dataframe(self, df, final_table_name, validate_and_format_func)
    elif ext == '.csv':
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        final_table_name = table_name if table_name else sanitize_table_name(base_name)
        logger.log_info(f'Đang import file CSV {file_path} vào bảng {final_table_name}')
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
    """Xử lý dữ liệu theo hàm validate, ghi vào DB."""
    
    reply = QMessageBox.question(
        self, "Xoá dữ liệu cũ?",  # Sửa lại câu hỏi cho chính xác
        f"Bạn có muốn xoá toàn bộ dữ liệu cũ trong bảng '{table_name}' trước khi import không?",
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
    )
    if reply == QMessageBox.StandardButton.Yes:
        try:
            with self.db_engine.connect() as conn:
                # Dùng text() để thực thi câu lệnh SQL và commit để lưu thay đổi
                conn.execute(text(f"DELETE FROM {table_name}"))
                conn.commit()
                logger.log_info(f"Đã xoá toàn bộ dữ liệu cũ trong bảng {table_name}.")
        except OperationalError:
            # Bảng chưa tồn tại, không sao cả, to_sql sẽ tạo mới.
            logger.log_warning(f"Bảng {table_name} không tồn tại. Bảng sẽ được tạo khi import.")
        except Exception as e:
            QMessageBox.critical(self, "Lỗi Xoá Dữ Liệu", f"Không thể xoá dữ liệu cũ từ bảng {table_name}:\n{e}")
            logger.log_error(f"Lỗi khi xoá dữ liệu từ bảng {table_name}: {e}")
            return  # Dừng lại nếu không xoá được như yêu cầu

    processed_df = validate_and_format_func(df)
    if processed_df is not None and not processed_df.empty:
        processed_df.to_sql(table_name, self.db_engine, if_exists='append', index=False)


def sanitize_table_name(name):
    """Chuẩn hóa tên bảng SQL (thay ký tự không hợp lệ, thêm tiền tố nếu bắt đầu bằng số)."""
    sanitized = re.sub(r'\W+', '_', name.replace(' ', '_').lower())
    if sanitized and sanitized[0].isdigit():
        sanitized = f'table_{sanitized}'
    return sanitized

# ======== Merge và So sánh 1A, SKD, AIMS chỉ trên DB ========

def merge_1a_from_db(db_engine):
    """
    Đọc Periods_1A và Market_Report_1A từ DB, merge và trả về các DataFrame.
    """
    periods = pd.read_sql("Periods_1A", db_engine)
    market = pd.read_sql("Market_Report_1A", db_engine)
    join_cols = ["OL", "FlightNbr", "OperationDate", "Frequency", "DEP", "ARR"]
    merged = pd.merge(periods, market, how="inner", on=join_cols)
    not_in_market = periods[~periods.set_index(join_cols).index.isin(market.set_index(join_cols).index)].reset_index(drop=True)
    not_in_periods = market[~market.set_index(join_cols).index.isin(periods.set_index(join_cols).index)].reset_index(drop=True)
    return merged, not_in_market, not_in_periods

def process_merge_1a(db_engine):
    """
    Merge Periods_1A và Market_Report_1A từ DB, lưu lại các bảng kết quả vào DB và trả về DataFrame
    """
    merged, not_in_market, not_in_periods = merge_1a_from_db(db_engine)
    merged.to_sql("Merged_1A", db_engine, if_exists="replace", index=False)
    not_in_market.to_sql("NOT_in_Market_Report", db_engine, if_exists="replace", index=False)
    not_in_periods.to_sql("NOT_in_Periods", db_engine, if_exists="replace", index=False)
    return merged, not_in_market, not_in_periods

def process_compare_aims_1a(db_engine):
    """
    Tạo Merged_1A và các bảng liên quan, trả về dict các DataFrame để xuất Excel
    """
    merged, not_in_market, not_in_periods = process_merge_1a(db_engine)
    return {
        "Merged_1A": merged,
        "NOT_in_Market_Report": not_in_market,
        "NOT_in_Periods": not_in_periods
    }

def compare_skd_and_1a(skd_data, merged_1a):
    """
    So sánh dữ liệu SKD_Data với Merged_1A:
    - EQT/CON: so trường cấu hình
    - TIM: so trường thời gian
    - CNL: các dòng có ở SKD_Data mà không có ở Merged_1A
    Trả về: compare_result, still_in_market_report
    """
    result_list = []
    still_in_market_report = None

    eqt_con_fields = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','ACV','SaleableCfg','C(S)','Y(S)','ACtype']
    eqt_con_fields_1a = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','ACV','SaleableCfg','C','Y','ACtype']
    tim_fields = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','STD','ACtype']
    cnl_fields = ['OL','FlightNbr','OperationDate','Frequency','DEP','ARR','Change code','Reason','SvType']

    all_required_cols = set(eqt_con_fields + eqt_con_fields_1a + tim_fields + cnl_fields)
    for col in all_required_cols:
        if col not in skd_data.columns:
            skd_data[col] = ""
        if col not in merged_1a.columns:
            merged_1a[col] = ""

    # 1. EQT hoặc CON: so trường cấu hình
    mask_eqt_con = skd_data['Change code'].isin(['EQT', 'CON'])
    if mask_eqt_con.any():
        skd_eqt_con = skd_data[eqt_con_fields].copy()
        skd_eqt_con = skd_eqt_con.rename(columns={'C(S)': 'C', 'Y(S)': 'Y'})
        merged_eqt_con = merged_1a[eqt_con_fields_1a].copy()
        merged_eqt_con = merged_eqt_con.astype(str)
        skd_eqt_con = skd_eqt_con[eqt_con_fields_1a].astype(str)
        df_eqt_con = pd.merge(skd_eqt_con, merged_eqt_con, how='inner', on=eqt_con_fields_1a)
        if not df_eqt_con.empty:
            df_eqt_con['Change code'] = 'EQT/CON'
            result_list.append(df_eqt_con)

    # 2. TIM: so trường thời gian
    mask_tim = skd_data['Change code'] == 'TIM'
    if mask_tim.any():
        skd_tim = skd_data[mask_tim][tim_fields].astype(str)
        merged_tim = merged_1a[tim_fields].astype(str)
        df_tim = pd.merge(skd_tim, merged_tim, how='inner', on=tim_fields)
        if not df_tim.empty:
            df_tim['Change code'] = 'TIM'
            result_list.append(df_tim)

    # 3. CNL: so trường bay bị huỷ
    mask_cnl = skd_data['Change code'] == 'CNL'
    if mask_cnl.any():
        fields = ['OL','FlightNbr','OperationDate','Frequency']
        skd_cnl = skd_data[mask_cnl].copy()
        merged_cnl = merged_1a[fields].copy()
        idx = ~skd_cnl.set_index(fields).index.isin(merged_cnl.set_index(fields).index)
        df_cnl = skd_cnl[idx][fields + ['DEP','ARR','Change code','Reason','SvType']]
        if not df_cnl.empty:
            result_list.append(df_cnl)
        idx_still = ~merged_cnl.set_index(fields).index.isin(skd_cnl.set_index(fields).index)
        still_in_market_report = merged_cnl[idx_still]
    else:
        still_in_market_report = None

    if result_list:
        compare_result = pd.concat(result_list, ignore_index=True)
    else:
        compare_result = pd.DataFrame(columns=[
            'OL','FlightNbr','OperationDate','Frequency','DEP','ARR','ACV','SaleableCfg',
            'C','Y','ACtype','STD','Change code','Reason','SvType'
        ])
    if 'still_in_market_report' not in locals():
        still_in_market_report = None

    return compare_result, still_in_market_report

def process_compare_skd_1a(db_engine):
    """
    So sánh SKD_Data với Merged_1A, lưu kết quả vào DB và trả về dict DataFrame để xuất Excel
    """
    merged, _, _ = process_merge_1a(db_engine)
    skd_data = pd.read_sql("SKD_Data", db_engine)
    merged_1a = pd.read_sql("Merged_1A", db_engine)

    compare_result, still_in_market_report = compare_skd_and_1a(skd_data, merged_1a)

    compare_result.to_sql("Compare_SKD_1A_Data", db_engine, if_exists="replace", index=False)
    if still_in_market_report is not None and not still_in_market_report.empty:
        still_in_market_report.to_sql("STILL_in_Market_Report", db_engine, if_exists="replace", index=False)
    return {
        "Compare_SKD_1A_Data": compare_result,
        "STILL_in_Market_Report": still_in_market_report
    }

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
    list_widget.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
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
        if 'OperationDate' in df.columns:
            df['OperationDate'] = pd.to_datetime(df['OperationDate'], format='%d-%b-%y', errors='coerce')
        df.to_excel(save_path, index=False)
        QMessageBox.information(
            parent,
            "Xuất thành công",
            f"Đã xuất bảng {table_name} ra file Excel.",
            buttons=QMessageBox.StandardButton.Ok,
        )