import sys
import pandas as pd
from PyQt6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout, QMessageBox, QFileDialog
)
from PyQt6.QtCore import Qt
from sqlalchemy import create_engine, inspect

import config
import libs  # dùng trực tiếp libs.<function>

class MyWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("10 Nút (2 cột × 5 hàng) với Hình nền - QVBoxLayout")
        self.setFixedSize(600, 500)

        self.config = config.Config()
        self.button_configs = self.config.button_configs
        self.button_names = [cfg["name"] for cfg in self.button_configs]
        self.background_images = self.config.background_images
        self.table_names = {
            cfg["name"]: cfg["table_name"]
            for cfg in self.config.button_configs
            if "table_name" in cfg and cfg["table_name"] is not None
        }

        self.db_engine = create_engine('sqlite:///imported_data.db')

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
                name = self.button_names[index]
                button = QPushButton(name)
                button.setFixedSize(200, 80)

                if name == "Thoát":
                    button.clicked.connect(QApplication.quit)
                elif name in ["1A Periods", "1A Market Report", "AIMS Data"]:
                    cfg = self.button_configs[index]
                    button.clicked.connect(lambda _, c=cfg: libs.on_import_clicked(self, c))
                elif name == "Đồng bộ":
                    button.clicked.connect(lambda: libs.export_to_excel_dialog(self))
                elif name == "Giúp đỡ":
                    button.clicked.connect(self.on_help_clicked)

                button.setStyleSheet(f"""
                    QPushButton {{
                        background-image: url('{self.background_images[index]}');
                        background-repeat: no-repeat;
                        background-position: center;
                        background-color: #444;
                        color: white;
                        font-weight: bold;
                        font-size: 14px;
                        border: 1px solid #ccc;
                        border-radius: 5px;
                    }}
                    QPushButton:pressed {{
                        background-color: #222;
                        padding-left: 2px;
                        padding-top: 2px;
                    }}
                """)

                col_layout.addWidget(button, alignment=Qt.AlignmentFlag.AlignHCenter)

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
            # Tải lại dữ liệu từ database và gán vào biến toàn cục
            libs.Periods_1A = pd.read_sql("Periods_1A", self.db_engine)
            libs.Market_Report_1A = pd.read_sql("Market_Report_1A", self.db_engine)
            libs.merge_tables_for_1A()

            # Ghi các bảng kết quả vào database
            libs.Merged_1A.to_sql("Merged_1A", self.db_engine, if_exists="replace", index=False)
            libs.NOT_in_Market_Report.to_sql("NOT_in_Market_Report", self.db_engine, if_exists="replace", index=False)
            libs.NOT_in_Periods.to_sql("NOT_in_Periods", self.db_engine, if_exists="replace", index=False)
        except Exception as e:
            QMessageBox.critical(self, "Lỗi xử lý dữ liệu", f"Lỗi khi tạo bảng: {str(e)}")
            return

        save_path, _ = QFileDialog.getSaveFileName(self, "Lưu kết quả dưới dạng Excel", "Result_AIMS_1A.xlsx", "Excel files (*.xlsx)")
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
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec())
