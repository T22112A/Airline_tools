from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QApplication, QMessageBox, QFileDialog
)
from sqlalchemy import create_engine, inspect
from config import Config
import pandas as pd
import libs
import functions  # file xử lý logic

class MainUI(QWidget):
    def __init__(self):
        super().__init__()

        self.db_engine = create_engine('sqlite:///mydatabase.db', echo=False)
        self.config = Config()
        self.button_configs = self.config.button_configs
        self.button_names = [cfg["name"] for cfg in self.button_configs]

        self.setWindowTitle("Ứng dụng chính")
        self.setup_ui()

    def setup_ui(self):
        main_layout = QVBoxLayout()
        main_layout.setContentsMargins(50, 50, 50, 50)
        main_layout.setSpacing(20)

        row_layout = QHBoxLayout()
        row_layout.setSpacing(40)

        for col in range(2):
            col_layout = QVBoxLayout()
            col_layout.setSpacing(15)

            for i in range(5):
                index = col * 5 + i
                if index >= len(self.button_names):
                    break

                name = self.button_names[index]
                button = QPushButton(name)
                button.setFixedSize(200, 80)

                if name == "Thoát":
                    button.clicked.connect(QApplication.quit)
                elif name in ["1A Periods", "1A Market Report", "AIMS Data", "SKD Data"]:
                    cfg = self.button_configs[index]
                    button.clicked.connect(lambda _, c=cfg: libs.on_import_clicked(self, c))
                elif name == "Xuất dữ liệu DB":
                    button.clicked.connect(lambda: libs.export_to_excel_dialog(self))
                elif name == "So sánh AIMS và 1A":
                    button.clicked.connect(self.on_compare_aims_1a_clicked)
                elif name == "So sánh SKD và 1A":
                    button.clicked.connect(self.on_compare_skd_1a_clicked)
                else:
                    button.setEnabled(False)

                col_layout.addWidget(button)
            row_layout.addLayout(col_layout)

        main_layout.addLayout(row_layout)
        self.setLayout(main_layout)

    def on_compare_aims_1a_clicked(self):
        inspector = inspect(self.db_engine)
        existing_tables = set(inspector.get_table_names())
        required_tables = {"Market_Report_1A", "Periods_1A"}
        missing = required_tables - existing_tables
        if missing:
            QMessageBox.warning(self, "Thiếu dữ liệu", f"Thiếu bảng: {', '.join(missing)}. Vui lòng import đủ dữ liệu.")
            return

        try:
            dataframes = functions.process_compare_aims_1a(self.db_engine)
        except Exception as e:
            QMessageBox.critical(self, "Lỗi xử lý", f"Lỗi khi xử lý so sánh AIMS-1A: {str(e)}")
            return

        self.save_excel_results(dataframes, default_name="Result_AIMS_1A.xlsx")

    def on_compare_skd_1a_clicked(self):
        inspector = inspect(self.db_engine)
        existing_tables = set(inspector.get_table_names())
        required_tables = {"Market_Report_1A", "Periods_1A", "SKD_Data"}
        missing = required_tables - existing_tables
        if missing:
            QMessageBox.warning(self, "Thiếu dữ liệu", f"Thiếu bảng: {', '.join(missing)}. Vui lòng import đủ dữ liệu.")
            return

        try:
            dataframes = functions.process_compare_skd_1a(self.db_engine)
        except Exception as e:
            QMessageBox.critical(self, "Lỗi xử lý", f"Lỗi khi xử lý so sánh SKD-1A: {str(e)}")
            return

        self.save_excel_results(dataframes, default_name="Compare_SKD_1A_Data.xlsx")

    def save_excel_results(self, dataframes, default_name):
        save_path, _ = QFileDialog.getSaveFileName(
            self,
            "Lưu kết quả dưới dạng Excel",
            default_name,
            "Excel files (*.xlsx)"
        )
        if not save_path:
            return

        if not save_path.lower().endswith(".xlsx"):
            save_path += ".xlsx"

        try:
            with pd.ExcelWriter(save_path, engine='xlsxwriter', datetime_format='dd-mmm-yy', date_format='dd-mmm-yy') as writer:
                for sheet_name, df in dataframes.items():
                    if df is not None and not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            QMessageBox.information(self, "Hoàn tất", f"Đã tạo bảng và lưu file: {save_path}")
        except Exception as e:
            QMessageBox.critical(self, "Lỗi ghi file", str(e))


if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    window = MainUI()
    window.show()
    sys.exit(app.exec())