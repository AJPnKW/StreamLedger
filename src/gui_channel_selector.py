#!/usr/bin/env python3
# ==============================================================================
# [FILE] src/gui_channel_selector.py
# [PROJECT] StreamLedger
# [ROLE] Channel Selector GUI - load M3U, searchable multi-select, save/load manual overrides
# [VERSION] v1.0
# [UPDATED] 2025-12-24
# ==============================================================================

import sys
import logging
import yaml
from pathlib import Path

# Add project root to path for direct run from src/
sys.path.append(str(Path(__file__).parent.parent))

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QLabel, QTreeWidget, QTreeWidgetItem
)
from PyQt6.QtCore import Qt

# Safe import after path fix
from filter_playlist import parse_m3u

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_DIR = Path(__file__).parent.parent / "config"
OVERRIDES_FILE = CONFIG_DIR / "manual_overrides.yaml"
M3U_OUTPUT = Path(__file__).parent.parent / "outputs" / "curated.m3u"

# Rest of your original code unchanged below
def load_overrides():
    if OVERRIDES_FILE.exists():
        with open(OVERRIDES_FILE, "r") as f:
            return yaml.safe_load(f) or {"include": [], "exclude": []}
    return {"include": [], "exclude": []}

def save_overrides(include, exclude):
    overrides = {"include": include, "exclude": exclude}
    CONFIG_DIR.mkdir(exist_ok=True)
    with open(OVERRIDES_FILE, "w") as f:
        yaml.safe_dump(overrides, f)
    logging.info(f"Saved overrides to {OVERRIDES_FILE}")

class ChannelSelector(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("StreamLedger - Channel Selector")
        self.resize(1000, 700)
        self.channels = []
        self.filtered = []
        self.overrides = load_overrides()

        layout = QVBoxLayout()

        # Search
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search:"))
        self.search_box = QLineEdit()
        self.search_box.textChanged.connect(self.filter_channels)
        search_layout.addWidget(self.search_box)
        layout.addLayout(search_layout)

        # Tree
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Channel", "Group", "tvg-id"])
        layout.addWidget(self.tree)

        # Buttons
        btn_layout = QHBoxLayout()
        save_btn = QPushButton("Save Overrides")
        save_btn.clicked.connect(self.save)
        btn_layout.addWidget(save_btn)
        layout.addLayout(btn_layout)

        self.setLayout(layout)
        self.load_channels()

    def load_channels(self):
        if not M3U_OUTPUT.exists():
            logging.error(f"{M3U_OUTPUT} not found. Run pipeline first.")
            return
        self.channels = parse_m3u(str(M3U_OUTPUT))
        self.filter_channels()

    def filter_channels(self):
        query = self.search_box.text().lower()
        self.tree.clear()
        self.filtered = [
            ch for ch in self.channels
            if query in ch["name"].lower() or query in ch.get("group", "").lower()
        ]
        for ch in self.filtered:
            item = QTreeWidgetItem([
                ch["name"],
                ch.get("group", ""),
                ch.get("tvg-id", "")
            ])
            if ch["name"] in self.overrides["include"]:
                item.setCheckState(0, Qt.CheckState.Checked)
            elif ch["name"] in self.overrides["exclude"]:
                item.setCheckState(0, Qt.CheckState.Unchecked)
            else:
                item.setCheckState(0, Qt.CheckState.Checked)  # default include
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            self.tree.addTopLevelItem(item)

    def save(self):
        include = []
        exclude = []
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            name = item.text(0)
            if item.checkState(0) == Qt.CheckState.Checked:
                include.append(name)
            else:
                exclude.append(name)
        save_overrides(include, exclude)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = ChannelSelector()
    win.show()
    sys.exit(app.exec())
