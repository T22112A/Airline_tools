import sys
from PyQt6.QtWidgets import (
    QApplication, QWidget, QPushButton, QVBoxLayout, QHBoxLayout
)
from PyQt6.QtCore import Qt
from sqlalchemy import create_engine

from config import Config
from libs import on_import_clicked, export_to_excel_dialog

class MyWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("10 Nút (2 cột × 5 hàng) với Hình nền - QVBoxLayout")
        self.setFixedSize(600, 500)

        self.config = Config()
        self.button_configs = self.config.button_configs
        self.button_names = [cfg["name"] for cfg in self.button_configs]
        self.background_images = self.config.background_images
        self.table_names = {
            cfg["name"]: cfg["table_name"]
            for cfg in self.config.button_configs
            if "table_name" in cfg and cfg["table_name"] is not None
}

        self.num_cols = self.config.default_num_cols
        self.required_cols = self.config.default_required_cols
        self.start_row = self.config.default_start_row
        self.import_all_sheets = self.config.default_import_all_sheets

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

                elif name in ["1A Periods", "1A Market Report"]:
                    button.clicked.connect(lambda _, n=name: on_import_clicked(self, n))

                elif name == "Đồng bộ":
                    button.clicked.connect(lambda: export_to_excel_dialog(self))

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

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow()
    window.show()
    sys.exit(app.exec())
