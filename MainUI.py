from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QApplication, QMessageBox, QFileDialog
)
from sqlalchemy import create_engine, inspect
from config import Config
import libs  # module chứa các hàm import/export
import pandas as pd


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
                    button.clicked.connect(self.on_help_clicked)
                else:
                    button.setEnabled(False)

                col_layout.addWidget(button)
            row_layout.addLayout(col_layout)

        main_layout.addLayout(row_layout)
        self.setLayout(main_layout)

    def on_help_clicked(self):
        inspector = inspect(self.db_engine)
        existing_tables = inspector.get_table_names()
        required_tables = {"Market_Report_1A", "Periods_1A"}
        missing = required_tables - set(existing_tables)

        if missing:
            QMessageBox.warning(self, "Thiếu dữ liệu", f"Thiếu bảng: {', '.join(missing)}. Vui lòng import đủ dữ liệu.")
            return

        try:
            libs.Periods_1A = pd.read_sql("Periods_1A", self.db_engine)
            libs.Market_Report_1A = pd.read_sql("Market_Report_1A", self.db_engine)
        except Exception as e:
            QMessageBox.critical(self, "Lỗi truy vấn", f"Lỗi khi đọc dữ liệu từ database: {str(e)}")
            return

        try:
            libs.merge_tables_for_1A()
        except Exception as e:
            QMessageBox.critical(self, "Lỗi gộp dữ liệu", f"Lỗi trong hàm merge_tables_for_1A(): {str(e)}")
            return

        try:
            libs.Merged_1A.to_sql("Merged_1A", self.db_engine, if_exists="replace", index=False)
            libs.NOT_in_Market_Report.to_sql("NOT_in_Market_Report", self.db_engine, if_exists="replace", index=False)
            libs.NOT_in_Periods.to_sql("NOT_in_Periods", self.db_engine, if_exists="replace", index=False)
        except Exception as e:
            QMessageBox.critical(self, "Lỗi ghi DB", f"Lỗi khi ghi bảng vào database: {str(e)}")
            return

        self.save_excel_results()


    def save_excel_results(self):
        save_path, _ = QFileDialog.getSaveFileName(
            self,
            "Lưu kết quả dưới dạng Excel",
            "Result_AIMS_1A.xlsx",
            "Excel files (*.xlsx)"
        )
        if not save_path:
            return

        if not save_path.lower().endswith(".xlsx"):
            save_path += ".xlsx"

        try:
            with pd.ExcelWriter(save_path, engine='xlsxwriter', datetime_format='dd-mmm-yy', date_format='dd-mmm-yy') as writer:
                dataframes = {
                    "Merged_1A": libs.Merged_1A,
                    "NOT_in_Market_Report": libs.NOT_in_Market_Report,
                    "NOT_in_Periods": libs.NOT_in_Periods
                }

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
