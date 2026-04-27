"""Interactive image canvas: displays a page, OCR token boxes, and lets the user
draw new bounding boxes or click existing tokens to assign them to a field.
"""
from __future__ import annotations
from typing import Callable, List, Optional

from PyQt5.QtCore import Qt, QRectF, QPointF, pyqtSignal
from PyQt5.QtGui import QPixmap, QPainter, QPen, QColor, QBrush, QFont
from PyQt5.QtWidgets import (
    QGraphicsView, QGraphicsScene, QGraphicsPixmapItem,
    QGraphicsRectItem, QGraphicsItem, QGraphicsTextItem,
)

from ..ocr_engine import Page, Token


FIELD_COLORS = [
    "#e6194B", "#3cb44b", "#4363d8", "#f58231", "#911eb4",
    "#42d4f4", "#f032e6", "#bfef45", "#fabed4", "#469990",
    "#9A6324", "#800000", "#aaffc3", "#808000", "#000075",
    "#a9a9a9",
]


class FieldBoxItem(QGraphicsRectItem):
    """A drawn field bounding box on the canvas."""

    def __init__(self, rect: QRectF, field_key: str, color: QColor):
        super().__init__(rect)
        self.field_key = field_key
        pen = QPen(color, 3)
        self.setPen(pen)
        self.setBrush(QBrush(QColor(color.red(), color.green(), color.blue(), 40)))
        self.setFlag(QGraphicsItem.ItemIsSelectable, True)
        self.setZValue(10)
        self._label = QGraphicsTextItem(field_key, self)
        self._label.setDefaultTextColor(color)
        f = QFont()
        f.setBold(True)
        f.setPointSize(11)
        self._label.setFont(f)
        self._label.setPos(rect.topLeft() + QPointF(2, -22))


class ImageCanvas(QGraphicsView):
    """Canvas with zoom/pan and rectangle drawing."""

    box_drawn = pyqtSignal(QRectF)  # emitted when user finishes drawing

    def __init__(self, parent=None):
        super().__init__(parent)
        self._scene = QGraphicsScene(self)
        self.setScene(self._scene)
        self.setRenderHint(QPainter.Antialiasing)
        self.setRenderHint(QPainter.SmoothPixmapTransform)
        self.setDragMode(QGraphicsView.ScrollHandDrag)
        self.setTransformationAnchor(QGraphicsView.AnchorUnderMouse)

        self._pix_item: Optional[QGraphicsPixmapItem] = None
        self._token_items: list[QGraphicsRectItem] = []
        self._field_items: list[FieldBoxItem] = []

        self._drawing = False
        self._origin: Optional[QPointF] = None
        self._rubber: Optional[QGraphicsRectItem] = None

        self._tokens: List[Token] = []
        self._on_token_clicked: Optional[Callable[[Token], None]] = None
        self._field_color_map: dict[str, QColor] = {}

    # ---------- public API ----------
    def load_page(self, page: Page, on_token_clicked: Callable[[Token], None]):
        self._scene.clear()
        self._token_items.clear()
        self._field_items.clear()
        self._tokens = page.tokens
        self._on_token_clicked = on_token_clicked

        pix = QPixmap(str(page.image_path))
        self._pix_item = self._scene.addPixmap(pix)
        self._scene.setSceneRect(QRectF(pix.rect()))

        # Draw token boxes (faint), so user can click them
        for t in page.tokens:
            r = QGraphicsRectItem(t.x, t.y, t.w, t.h)
            r.setPen(QPen(QColor(80, 130, 255, 180), 1))
            r.setBrush(QBrush(QColor(80, 130, 255, 25)))
            r.setData(0, t)  # store token reference
            r.setZValue(5)
            self._scene.addItem(r)
            self._token_items.append(r)

        self.resetTransform()
        self.fitInView(self._pix_item, Qt.KeepAspectRatio)

    def color_for_field(self, field_key: str) -> QColor:
        if field_key not in self._field_color_map:
            idx = len(self._field_color_map) % len(FIELD_COLORS)
            self._field_color_map[field_key] = QColor(FIELD_COLORS[idx])
        return self._field_color_map[field_key]

    def add_field_box(self, rect: QRectF, field_key: str) -> FieldBoxItem:
        # remove previous box for same field (one box per field)
        for item in list(self._field_items):
            if item.field_key == field_key:
                self._scene.removeItem(item)
                self._field_items.remove(item)
        color = self.color_for_field(field_key)
        item = FieldBoxItem(rect, field_key, color)
        self._scene.addItem(item)
        self._field_items.append(item)
        return item

    def remove_field_box(self, field_key: str):
        for item in list(self._field_items):
            if item.field_key == field_key:
                self._scene.removeItem(item)
                self._field_items.remove(item)

    def get_field_boxes(self) -> dict[str, tuple[int, int, int, int]]:
        out = {}
        for item in self._field_items:
            r = item.rect()
            out[item.field_key] = (int(r.x()), int(r.y()), int(r.width()), int(r.height()))
        return out

    def set_drawing_mode(self, on: bool):
        self._drawing = on
        self.setDragMode(QGraphicsView.NoDrag if on else QGraphicsView.ScrollHandDrag)
        self.setCursor(Qt.CrossCursor if on else Qt.ArrowCursor)

    # ---------- events ----------
    def wheelEvent(self, event):
        factor = 1.2 if event.angleDelta().y() > 0 else 1 / 1.2
        self.scale(factor, factor)

    def mousePressEvent(self, event):
        if self._drawing and event.button() == Qt.LeftButton:
            self._origin = self.mapToScene(event.pos())
            self._rubber = QGraphicsRectItem(QRectF(self._origin, self._origin))
            self._rubber.setPen(QPen(QColor("#ff8800"), 2, Qt.DashLine))
            self._rubber.setZValue(20)
            self._scene.addItem(self._rubber)
            return
        # token click in normal mode
        if not self._drawing and event.button() == Qt.LeftButton:
            scene_pos = self.mapToScene(event.pos())
            item = self._scene.itemAt(scene_pos, self.transform())
            if isinstance(item, QGraphicsRectItem) and item.data(0) is not None:
                token: Token = item.data(0)
                if self._on_token_clicked:
                    self._on_token_clicked(token)
                return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._drawing and self._rubber and self._origin:
            cur = self.mapToScene(event.pos())
            self._rubber.setRect(QRectF(self._origin, cur).normalized())
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if self._drawing and self._rubber:
            rect = self._rubber.rect()
            self._scene.removeItem(self._rubber)
            self._rubber = None
            self._origin = None
            if rect.width() > 5 and rect.height() > 5:
                self.box_drawn.emit(rect)
            return
        super().mouseReleaseEvent(event)
