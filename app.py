import sys
from pathlib import Path
import sqlite3
import pandas as pd

from PySide6.QtGui import QColor, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QLabel,
    QPushButton,
    QFileDialog,
    QVBoxLayout,
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QFrame,
)
from PySide6.QtCore import Qt

from logic import (
    process_uploaded_files,
    evaluate_payment_across_all_products,
    list_product_dbs,
)


class PaymentApp(QWidget):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Customer 360 Insight")
        logo_path = Path("360logo.png")
        if logo_path.exists():
            self.setWindowIcon(QIcon(str(logo_path)))
        self.setMinimumSize(1100, 650)

        self.disbursed_path = None
        self.collection_path = None
        self.last_sql_df = None

        self.apply_styles()
        self.init_ui()

    # =======================
    # STYLES (UI ONLY)
    # =======================
    def apply_styles(self):
        self.setStyleSheet(
            """
            QWidget {
                font-family: 'Segoe UI', 'Inter', Arial;
                font-size: 14px;
                background-color: #f6f7fb;
                color: #111827;
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
        """
        )

    # =======================
    # UI LAYOUT
    # =======================
    def init_ui(self):
        main_layout = QVBoxLayout()
        main_layout.setSpacing(14)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # ---- Upload ----
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

        # ---- PAN Search ----
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

        # ---- SQL Query ----
        sql_label = QLabel("SQL Query")
        sql_label.setProperty("class", "section")
        main_layout.addWidget(sql_label)

        sql_layout = QHBoxLayout()

        self.sql_input = QLineEdit()
        self.sql_input.setPlaceholderText(
            "e.g. SELECT * FROM ELI d LEFT JOIN collection c ON d.LeadID = c.LeadID"
        )

        self.run_sql_btn = QPushButton("Run Query")
        self.run_sql_btn.clicked.connect(self.run_sql_query)

        self.export_sql_btn = QPushButton("Export Result")
        self.export_sql_btn.setEnabled(False)
        self.export_sql_btn.clicked.connect(self.export_sql_result)

        sql_layout.addWidget(self.sql_input)
        sql_layout.addWidget(self.run_sql_btn)
        sql_layout.addWidget(self.export_sql_btn)

        main_layout.addLayout(sql_layout)
        main_layout.addWidget(self.hline())

        # ---- Results ----
        results_label = QLabel("Results")
        results_label.setProperty("class", "section")
        main_layout.addWidget(results_label)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.verticalHeader().setVisible(False)
        self.table.setShowGrid(False)
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeToContents
        )

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
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Disbursed CSV", "", "CSV Files (*.csv)"
        )
        if path:
            self.disbursed_path = Path(path)
            self.disbursed_btn.setText(f"Disbursed: {Path(path).name}")

    def load_collection(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Select Collection CSV", "", "CSV Files (*.csv)"
        )
        if path:
            self.collection_path = Path(path)
            self.collection_btn.setText(f"Collection: {Path(path).name}")

    def process_files(self):
        if not self.disbursed_path or not self.collection_path:
            QMessageBox.warning(self, "Missing Files", "Please upload both CSV files.")
            return
        try:
            product, conn = process_uploaded_files(
                self.disbursed_path, self.collection_path
            )
            conn.close()
            QMessageBox.information(
                self, "Success", f"Product DB ready: {product}.db"
            )
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
    # SQL QUERY ENGINE
    # =======================
    def run_sql_query(self):
        raw_query = self.sql_input.text().strip()
        if not raw_query:
            QMessageBox.warning(self, "Invalid Query", "SQL query cannot be empty.")
            return

        try:
            # Create ONE master connection
            conn = sqlite3.connect(":memory:")

            # Attach all product DBs
            for db in list_product_dbs():
                alias = db.stem.upper()
                conn.execute(f"ATTACH DATABASE '{db}' AS {alias}")

            # Execute user query ONCE
            df = pd.read_sql_query(raw_query, conn)

            conn.close()

            if df.empty:
                QMessageBox.information(self, "No Results", "Query returned no rows.")
                self.table.clear()
                self.export_sql_btn.setEnabled(False)
                return

            self.last_sql_df = df
            self.populate_table(df)
            self.export_sql_btn.setEnabled(True)

        except Exception as e:
            QMessageBox.critical(self, "SQL Error", str(e))


    def export_sql_result(self):
        if self.last_sql_df is None or self.last_sql_df.empty:
            QMessageBox.warning(self, "Nothing to Export", "No query result available.")
            return

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Query Result",
            "sql_query_result.csv",
            "CSV Files (*.csv)",
        )

        if path:
            try:
                self.last_sql_df.to_csv(path, index=False)
                QMessageBox.information(self, "Exported", f"Saved to {path}")
            except Exception as e:
                QMessageBox.critical(self, "Export Failed", str(e))

    # =======================
    # TABLE POPULATION
    # =======================
    def populate_table(self, df):
        self.table.clear()

        rows = len(df)
        cols = len(df.columns) + 1

        self.table.setRowCount(rows)
        self.table.setColumnCount(cols)
        self.table.setHorizontalHeaderLabels(
            ["S. No."] + df.columns.tolist()
        )

        PRODUCT_COLORS = {
            "ELI": QColor("#fde2e2"),
            "NBL": QColor("#e0f2fe"),
            "CPY": QColor("#ecfdf5"),
            "LDR": QColor("#f3e8ff"),
        }

        product_col_index = (
            df.columns.get_loc("Product") if "Product" in df.columns else None
        )

        for r in range(rows):
            index_item = QTableWidgetItem(str(r + 1))
            index_item.setTextAlignment(Qt.AlignCenter)

            row_color = None
            if product_col_index is not None:
                product = str(df.iat[r, product_col_index]).upper()
                row_color = PRODUCT_COLORS.get(product)

            if row_color:
                index_item.setBackground(row_color)

            self.table.setItem(r, 0, index_item)

            for c, col in enumerate(df.columns):
                val = str(df.iat[r, c])
                item = QTableWidgetItem(val)
                item.setToolTip(val)
                if row_color:
                    item.setBackground(row_color)
                self.table.setItem(r, c + 1, item)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = PaymentApp()
    window.show()
    sys.exit(app.exec())
