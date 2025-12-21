"""
StreamLedger v1.0 - Channel Selector GUI
Purpose: Load merged M3U, allow searchable multi-select,
         save/load manual overrides for whitelist control.
"""

import sys
import logging
import yaml
from pathlib import Path
from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QFileDialog, QTreeWidget, QTreeWidgetItem,
    QLineEdit, QLabel
)
from PyQt6.QtCore import Qt

from src.filter_playlist import parse_m3u

# ---- paths ----
BASE_DIR = Path(__file__).resolve().parents[1]
CONFIG_DIR = BASE_DIR / "config"
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ---- logging ----
logging.basicConfig(
    filename=LOG_DIR / "gui_channel_selector.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

class ChannelSelector(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("StreamLedger – Channel Selector")
        self.resize(1000, 650)

        self.channels = []

        layout = QVBoxLayout(self)

        header = QLabel("Search & Select Channels (manual overrides)")
        header.setStyleSheet("font-weight: bold;")
        layout.addWidget(header)

        self.search = QLineEdit()
        self.search.setPlaceholderText("Search tvg-id / name / group…")
        self.search.textChanged.connect(self.apply_filter)
        layout.addWidget(self.search)

        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Channel"])
        self.tree.setUniformRowHeights(True)
        layout.addWidget(self.tree)

        btns = QHBoxLayout()
        load_btn = QPushButton("Load M3U")
        save_btn = QPushButton("Save Overrides")
        load_overrides_btn = QPushButton("Load Overrides")

        load_btn.clicked.connect(self.load_m3u)
        save_btn.clicked.connect(self.save_overrides)
        load_overrides_btn.clicked.connect(self.load_overrides)

        btns.addWidget(load_btn)
        btns.addWidget(load_overrides_btn)
        btns.addWidget(save_btn)
        layout.addLayout(btns)

    def load_m3u(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Open M3U", "", "M3U Playlists (*.m3u)"
        )
        if not path:
            return

        logging.info("Loading M3U: %s", path)
        self.channels = parse_m3u(Path(path))
        self.tree.clear()

        for ch in self.channels:
            label = f"{ch.get('tvg-id','')} | {ch['name']} | {ch.get('group','')}"
            item = QTreeWidgetItem([label])
            item.setCheckState(0, Qt.CheckState.Checked)
            self.tree.addTopLevelItem(item)

    def apply_filter(self, text):
        text = text.lower()
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            item.setHidden(text not in item.text(0).lower())

    def save_overrides(self):
        selected = []
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if item.checkState(0) == Qt.CheckState.Checked:
                selected.append(item.text(0))

        out = CONFIG_DIR / "manual_overrides.yaml"
        with out.open("w", encoding="utf-8") as f:
            yaml.safe_dump(
                {"version": 1.0, "selected": selected},
                f,
                sort_keys=False
            )

        logging.info("Saved %d overrides", len(selected))

    def load_overrides(self):
        path = CONFIG_DIR / "manual_overrides.yaml"
        if not path.exists():
            logging.warning("No overrides file found")
            return

        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        selected = set(data.get("selected", []))

        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            item.setCheckState(
                0,
                Qt.CheckState.Checked if item.text(0) in selected else Qt.CheckState.Unchecked
            )

        logging.info("Loaded overrides: %d items", len(selected))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ChannelSelector()
    win.show()
    sys.exit(app.exec())
