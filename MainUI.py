from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QApplication, QMessageBox
)
from sqlalchemy import create_engine
from config import Config
import libs  # module chứa các hàm import/export của bạn

class MainUI(QWidget):
    def __init__(self):
        super().__init__()

        # Khởi tạo kết nối SQLite bằng SQLAlchemy
        self.db_engine = create_engine('sqlite:///mydatabase.db', echo=False)

        # Lấy config nút từ config.py
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

        # Tạo nút theo config, 2 cột x 5 nút
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
                else:
                    # Disable các nút không có sự kiện gán
                    button.setEnabled(False)

                col_layout.addWidget(button)
            row_layout.addLayout(col_layout)

        main_layout.addLayout(row_layout)
        self.setLayout(main_layout)

    def on_help_clicked(self):
        QMessageBox.information(
            self,
            "Giúp đỡ",
            "Liên hệ admin để được hỗ trợ.",
            buttons=QMessageBox.StandardButton.Ok,
            defaultButton=QMessageBox.StandardButton.NoButton,
        )

if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    window = MainUI()
    window.show()
    sys.exit(app.exec())
