"""MIDAS mapper — convertit une ManifestRow (ou dict équivalent) en dict 43 colonnes
au format AGL/MIDAS prêt à exporter vers Excel.

Une ligne MIDAS = 1 BL × 1 split conteneur. Les colonnes vides correspondent aux
champs saisis manuellement par l'équipe d'intégration (Numéro escale, Index, Range)
ou non disponibles dans le manifeste.
"""
from __future__ import annotations

import re
import yaml
from pathlib import Path
from typing import Any

from .config import resource_dir


# Ordre exact des colonnes MIDAS (cf. spécification AGL)
MIDAS_COLUMNS: list[str] = [
    "Numéro escale",
    "Consignataire",
    "Navire",
    "Armateur escale",
    "Numéro BL",
    "I_IMP_E_EXP",
    "Type fret",
    "Armateur BL",
    "Port de chargement",
    "Port de déchargement",
    "Port transbo1",
    "Port transbo2",
    "Port escale",
    "Code transitaire",
    "Transitaire",
    "chargeur.code",
    "Chargeur",
    "CODE_MODE_EXPEDITION",
    "Destinataire",
    "Manutentionaire",
    "Code marchandise",
    "Libellé marchandise",
    "POIDS_MARCHANDISE",
    "Nombre de TC",
    "NOMBRE_TEU",
    "Nombre de colis",
    "volume marchandise",
    "CODE_CONDIT",
    "Pays de prise en charge",
    "code_lieu_prise_charge",
    "Lieu de prise en charge",
    "Pays de livraison",
    "Code lieu livraison",
    "Lieu de livraison",
    "Année escale",
    "Mois escale",
    "Jour escale",
    "Qualifié?",
    "Index",
    "Range",
    "code armateur",
    "code pays",
    "système",
]

_LOOKUPS_CACHE: dict | None = None


def _lookups() -> dict:
    global _LOOKUPS_CACHE
    if _LOOKUPS_CACHE is None:
        path = resource_dir() / "config" / "midas_lookups.yaml"
        _LOOKUPS_CACHE = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return _LOOKUPS_CACHE


def _norm(s: str) -> str:
    """Lowercase + strip + collapse whitespace for lookup matching."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _resolve_carrier(bl_number: str, fallback: str = "") -> dict:
    """Return {code, name} from BL prefix; fallback name if unknown."""
    table = _lookups().get("bl_prefix_to_carrier", {})
    if bl_number:
        prefix = bl_number[:4].lower()
        if prefix in table:
            return table[prefix]
        prefix3 = bl_number[:3].lower()
        for k, v in table.items():
            if k.startswith(prefix3):
                return v
    # Fallback: try to resolve code from the carrier name
    if fallback:
        norm_fb = _norm(fallback)
        for v in table.values():
            if _norm(v.get("name", "")) == norm_fb:
                return v
    return {"code": "", "name": fallback}


def _resolve_container(container_type: str) -> dict:
    """Return {code_condit, teu} from container ISO type (e.g. '40HC')."""
    key = _norm(container_type).replace(" ", "")
    table = _lookups().get("container_type_to_midas", {})
    if key in table:
        return table[key]
    # Fallback: parse size + assume general
    m = re.match(r"^(\d{2})", key)
    if m:
        size = m.group(1)
        teu = 2 if size == "40" or size == "45" else 1
        return {"code_condit": f"{size}S", "teu": teu}
    return {"code_condit": "", "teu": 0}


def _resolve_port(port_name: str) -> dict:
    """Return {code, country_code, country, city} from port name."""
    key = _norm(port_name)
    table = _lookups().get("port_to_country", {})
    if key in table:
        return table[key]
    # Try contains match (e.g. "Tanger Med Port" → "tanger med")
    for k, v in table.items():
        if k in key:
            return v
    return {"code": "", "country_code": "", "country": "", "city": port_name}


def _resolve_movement(movement: str) -> str:
    key = _norm(movement)
    table = _lookups().get("movement_to_mode", {})
    if key in table:
        return table[key]
    return _lookups().get("defaults", {}).get("code_mode_expedition", "FCLFCL")


def _parse_date(date_str: str) -> tuple[str, str, str]:
    """Parse arrival date → (year, month_fr, day). Accepts DD/MM/YYYY or DD-MM-YYYY."""
    if not date_str:
        return "", "", ""
    m = re.search(r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})", date_str)
    if not m:
        return "", "", ""
    day, month, year = m.group(1), m.group(2), m.group(3)
    if len(year) == 2:
        year = "20" + year
    months = _lookups().get("months_fr", [])
    try:
        month_fr = months[int(month) - 1] if 1 <= int(month) <= 12 else month
    except (ValueError, IndexError):
        month_fr = month
    return year, month_fr, day


def map_to_midas(row: dict[str, Any], static_overrides: dict | None = None) -> dict[str, Any]:
    """Transform one ManifestRow dict into a MIDAS row dict (42 columns).

    static_overrides: optional dict to force certain columns (e.g. {"Consignataire": "AGL", "Manutentionaire": "BOLLORE"})
    """
    defaults = _lookups().get("defaults", {})
    overrides = static_overrides or {}

    def _s(key: str) -> str:
        v = row.get(key)
        return "" if v is None else str(v).strip()

    bl_number   = _s("bl_number")
    vessel      = _s("vessel")
    pol         = _s("port_of_loading")
    pod         = _s("port_of_discharge")
    delivery    = _s("place_of_delivery") or pod
    acceptance  = _s("place_of_acceptance") or pol
    shipper     = _s("shipper")
    consignee   = _s("consignee")
    forwarder   = _s("freight_forwarder")
    description = _s("description")
    weight      = _s("weight")
    volume      = _s("volume")
    pack_qty    = _s("pack_qty")
    container   = _s("container_number")
    ctr_type    = _s("container_type")
    movement    = _s("movement")

    carrier   = _resolve_carrier(bl_number, fallback=row.get("_shipowner", ""))
    container_info = _resolve_container(ctr_type)
    pol_info  = _resolve_port(pol)
    deliv_info = _resolve_port(delivery)
    accept_info = _resolve_port(acceptance) if acceptance else pol_info
    year, month_fr, day = _parse_date(row.get("date_of_arrival", ""))

    midas: dict[str, Any] = {
        "Numéro escale":            "",                              # saisi par équipe
        "Consignataire":            defaults.get("consignataire", ""),
        "Navire":                   vessel,
        "Armateur escale":          carrier["name"],
        "Numéro BL":                bl_number,
        "I_IMP_E_EXP":              defaults.get("i_imp_e_exp", "I"),
        "Type fret":                row.get("bl_type", "") or "210530003",
        "Armateur BL":              carrier["name"],
        "Port de chargement":       pol_info["city"] or pol,
        "Port de déchargement":     deliv_info["city"] or pod,
        "Port transbo1":            "",
        "Port transbo2":            "",
        "Port escale":              defaults.get("port_escale", "ABIDJAN"),
        "Code transitaire":         "",                              # saisi
        "Transitaire":              forwarder,
        "chargeur.code":            "",                              # saisi
        "Chargeur":                 shipper,
        "CODE_MODE_EXPEDITION":     _resolve_movement(movement),
        "Destinataire":             consignee,
        "Manutentionaire":          "",                              # saisi
        "Code marchandise":         "",                              # saisi
        "Libellé marchandise":      description,
        "POIDS_MARCHANDISE":        weight,
        "Nombre de TC":             "1" if container else "",
        "NOMBRE_TEU":               str(container_info["teu"]) if container_info["teu"] else "",
        "Nombre de colis":          pack_qty,
        "volume marchandise":       volume,
        "CODE_CONDIT":              container_info["code_condit"],
        "Pays de prise en charge":  accept_info["country"],
        "code_lieu_prise_charge":   accept_info["code"],
        "Lieu de prise en charge":  accept_info["city"] or acceptance,
        "Pays de livraison":        deliv_info["country"],
        "Code lieu livraison":      deliv_info["code"],
        "Lieu de livraison":        deliv_info["city"] or delivery,
        "Année escale":             year,
        "Mois escale":              month_fr,
        "Jour escale":              day,
        "Qualifié?":                defaults.get("qualifie", "Oui"),
        "Index":                    "",                              # saisi
        "Range":                    "",                              # saisi
        "code armateur":            carrier["code"],
        "code pays":                deliv_info["country_code"],
        "système":                  defaults.get("systeme", "MIDAS"),
    }

    # Apply user overrides
    for k, v in overrides.items():
        if k in midas:
            midas[k] = v
    return midas


def map_rows_to_midas(rows: list[dict], static_overrides: dict | None = None) -> list[dict]:
    """Vectorised version. Returns list of MIDAS dicts in the canonical column order."""
    return [map_to_midas(r, static_overrides) for r in rows]
