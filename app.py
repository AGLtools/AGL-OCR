"""AGL OCR launcher."""
import sys
from PyQt5.QtWidgets import QApplication

from src.ui.main_window import MainWindow, AGL_STYLE
from src.maintenance import run_cleanup
from src.config import DATA_DIR


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(AGL_STYLE)
    # Best-effort housekeeping (never blocks startup).
    try:
        run_cleanup(DATA_DIR)
    except Exception:
        pass
    win = MainWindow()
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
