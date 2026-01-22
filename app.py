import sys
from pathlib import Path
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QApplication, QWidget, QLabel, QPushButton, QFileDialog,
    QVBoxLayout, QHBoxLayout, QLineEdit, QMessageBox, QTableWidget,
    QTableWidgetItem, QHeaderView, QFrame
)
from PySide6.QtCore import Qt

from logic import (
    process_uploaded_files,
    evaluate_payment_across_all_products
)


class PaymentApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Payment Status Analyzer")
        self.setMinimumSize(1100, 650)

        self.disbursed_path = None
        self.collection_path = None

        self.apply_styles()
        self.init_ui()

    # =======================
    # STYLES (UI ONLY)
    # =======================
    def apply_styles(self):
        self.setStyleSheet("""
            QWidget {
                font-family: 'Segoe UI', 'Inter', Arial;
                font-size: 14px;
                background-color: #f6f7fb;
                color: #111827;
            }

            QLabel#TitleLabel {
                font-size: 26px;
                font-weight: 600;
                color: #1f2937;
                padding: 12px 0;
            }

            QLabel.section {
                font-size: 15px;
                font-weight: 600;
                color: #374151;
            }

            QPushButton {
                background-color: #2563eb;
                color: white;
                border-radius: 6px;
                padding: 8px 16px;
                font-weight: 500;
            }

            QPushButton:hover {
                background-color: #1d4ed8;
            }

            QPushButton:disabled {
                background-color: #9ca3af;
            }

            QLineEdit {
                padding: 8px;
                border: 1px solid #d1d5db;
                border-radius: 6px;
                background-color: white;
            }

            QLineEdit:focus {
                border: 1px solid #2563eb;
            }

            QTableWidget {
                background-color: white;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
            }

            QHeaderView::section {
                background-color: #f1f5f9;
                padding: 8px;
                border: none;
                font-weight: 600;
                color: #374151;
            }

            QTableWidget::item {
                padding: 6px;
            }
        """)

    # =======================
    # UI LAYOUT
    # =======================
    def init_ui(self):
        main_layout = QVBoxLayout()
        main_layout.setSpacing(14)
        main_layout.setContentsMargins(20, 20, 20, 20)

        title = QLabel("Payment Status Analyzer")
        title.setObjectName("TitleLabel")
        title.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(title)

        upload_label = QLabel("Upload Files")
        upload_label.setProperty("class", "section")
        main_layout.addWidget(upload_label)

        file_layout = QHBoxLayout()
        self.disbursed_btn = QPushButton("Upload Disbursed CSV")
        self.disbursed_btn.clicked.connect(self.load_disbursed)

        self.collection_btn = QPushButton("Upload Collection CSV")
        self.collection_btn.clicked.connect(self.load_collection)

        self.process_btn = QPushButton("Process Files")
        self.process_btn.clicked.connect(self.process_files)

        file_layout.addWidget(self.disbursed_btn)
        file_layout.addWidget(self.collection_btn)
        file_layout.addStretch()
        file_layout.addWidget(self.process_btn)

        main_layout.addLayout(file_layout)
        main_layout.addWidget(self.hline())

        pan_label = QLabel("PAN Search")
        pan_label.setProperty("class", "section")
        main_layout.addWidget(pan_label)

        pan_layout = QHBoxLayout()
        self.pan_input = QLineEdit()
        self.pan_input.setPlaceholderText("Enter PAN (e.g. BBUPM2364P)")

        self.search_btn = QPushButton("Search PAN")
        self.search_btn.clicked.connect(self.search_pan)

        pan_layout.addWidget(self.pan_input)
        pan_layout.addWidget(self.search_btn)

        main_layout.addLayout(pan_layout)
        main_layout.addWidget(self.hline())

        results_label = QLabel("Results")
        results_label.setProperty("class", "section")
        main_layout.addWidget(results_label)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setShowGrid(False)

        self.table.setHorizontalScrollMode(QTableWidget.ScrollPerPixel)
        self.table.setVerticalScrollMode(QTableWidget.ScrollPerPixel)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.table.horizontalScrollBar().setVisible(True)

        main_layout.addWidget(self.table)
        self.setLayout(main_layout)

    def hline(self):
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setFrameShadow(QFrame.Sunken)
        line.setStyleSheet("color: #e5e7eb;")
        return line

    # =======================
    # ACTIONS
    # =======================
    def load_disbursed(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Disbursed CSV", "", "CSV Files (*.csv)")
        if path:
            self.disbursed_path = Path(path)
            self.disbursed_btn.setText(f"Disbursed: {Path(path).name}")

    def load_collection(self):
        path, _ = QFileDialog.getOpenFileName(self, "Select Collection CSV", "", "CSV Files (*.csv)")
        if path:
            self.collection_path = Path(path)
            self.collection_btn.setText(f"Collection: {Path(path).name}")

    def process_files(self):
        if not self.disbursed_path or not self.collection_path:
            QMessageBox.warning(self, "Missing Files", "Please upload both CSV files.")
            return
        try:
            product, conn = process_uploaded_files(self.disbursed_path, self.collection_path)
            conn.close()
            QMessageBox.information(self, "Success", f"Product DB ready: {product}.db")
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    def search_pan(self):
        pan = self.pan_input.text().strip()
        if not pan:
            QMessageBox.warning(self, "Invalid PAN", "PAN cannot be empty.")
            return
        try:
            result = evaluate_payment_across_all_products(pan)
            df = result["table"]
            if df.empty:
                QMessageBox.information(self, "No Results", "No records found.")
                self.table.clear()
                return
            self.populate_table(df)
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))

    # =======================
    # TABLE POPULATION (WITH INDEX)
    # =======================
    def populate_table(self, df):
        self.table.clear()

        total_rows = len(df)
        total_cols = len(df.columns) + 1  # +1 for index

        self.table.setRowCount(total_rows)
        self.table.setColumnCount(total_cols)

        headers = ["S. No."] + df.columns.tolist()
        self.table.setHorizontalHeaderLabels(headers)

        PRODUCT_COLORS = {
            "ELI": QColor("#fde2e2"),
            "NBL": QColor("#e0f2fe"),
            "CPY": QColor("#ecfdf5"),
            "LDR": QColor("#f3e8ff"),
        }

        product_col_index = (
            df.columns.get_loc("Product") if "Product" in df.columns else None
        )

        for row in range(total_rows):
            # Index column
            index_item = QTableWidgetItem(str(row + 1))
            index_item.setTextAlignment(Qt.AlignCenter)

            product_value = (
                str(df.iat[row, product_col_index]).upper()
                if product_col_index is not None
                else None
            )
            row_color = PRODUCT_COLORS.get(product_value)

            if row_color:
                index_item.setBackground(row_color)

            self.table.setItem(row, 0, index_item)

            for col in range(len(df.columns)):
                value = str(df.iat[row, col])
                item = QTableWidgetItem(value)
                item.setToolTip(value)

                if row_color:
                    item.setBackground(row_color)

                self.table.setItem(row, col + 1, item)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PaymentApp()
    window.show()
    sys.exit(app.exec())
