"""Synthetic data generator for a Tawal-style tower company (KSA telecom
infrastructure): sites, towers, tenancy slots, tenant leases, equipment, fiber
/microwave links, field operations, alarms, and energy.

Deterministic (seeded) and stdlib-only. Writes a landing-zone layout that the
bronze pipeline (Auto Loader) and the app's "seed from a volume" feature read:

    out/landing/<entity>/<entity>[_<part>].csv | .json (JSON lines)

A few deliberate data-quality issues are injected into the high-volume event
feeds (exact duplicates, negative meter readings, inconsistent casing) so the
silver layer has something real to cleanse.

Usage:  python generate_tawal.py [out_dir]
"""

from __future__ import annotations

import csv
import json
import os
import random
import sys
from datetime import date, datetime, timedelta

SEED = 20261020
rng = random.Random(SEED)

OUT = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "out")
LANDING = os.path.join(OUT, "landing")

OPS_START = date(2026, 7, 1)
OPS_END = date(2026, 9, 30)
BILL_MONTHS = [date(2025, 10, 1) + timedelta(days=31 * i) for i in range(12)]
BILL_MONTHS = [d.replace(day=1) for d in BILL_MONTHS]


def write_csv(entity: str, rows: list[dict], part: str | None = None) -> None:
    d = os.path.join(LANDING, entity)
    os.makedirs(d, exist_ok=True)
    name = f"{entity}_{part}.csv" if part else f"{entity}.csv"
    with open(os.path.join(d, name), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_jsonl(entity: str, rows: list[dict]) -> None:
    d = os.path.join(LANDING, entity)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{entity}.json"), "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def rand_date(a: date, b: date) -> date:
    return a + timedelta(days=rng.randint(0, max(0, (b - a).days)))


def ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def weighted(choices: list[tuple]):
    vals, wts = zip(*choices)
    return rng.choices(vals, weights=wts, k=1)[0]


# ─── Reference data ──────────────────────────────────────────────────────────
REGIONS = [
    # id, code, name, arabic, population, area_km2
    (1, "RUH", "Riyadh", "الرياض", 8_591_748, 404_240),
    (2, "MKH", "Makkah", "مكة المكرمة", 8_021_463, 153_128),
    (3, "EP", "Eastern Province", "المنطقة الشرقية", 5_125_254, 672_522),
    (4, "MED", "Madinah", "المدينة المنورة", 2_137_983, 151_990),
    (5, "ASR", "Asir", "عسير", 2_024_285, 76_693),
    (6, "QSM", "Qassim", "القصيم", 1_336_179, 58_046),
    (7, "TBK", "Tabuk", "تبوك", 886_036, 146_072),
    (8, "HAI", "Hail", "حائل", 746_406, 103_887),
    (9, "JZN", "Jazan", "جازان", 1_404_997, 11_671),
    (10, "NJR", "Najran", "نجران", 592_300, 119_000),
    (11, "BAH", "Al Bahah", "الباحة", 339_174, 9_921),
    (12, "JOF", "Al Jawf", "الجوف", 595_822, 100_212),
    (13, "NBR", "Northern Borders", "الحدود الشمالية", 373_577, 111_797),
]
REGION_CODE = {r[0]: r[1] for r in REGIONS}

CITIES = [
    # name, region_id, lat, lon, tier, coastal, site_weight
    ("Riyadh", 1, 24.7136, 46.6753, "Tier 1", False, 60),
    ("Al Kharj", 1, 24.1556, 47.3346, "Tier 2", False, 6),
    ("Ad Diriyah", 1, 24.7333, 46.5750, "Tier 2", False, 4),
    ("Al Majmaah", 1, 25.9039, 45.3456, "Tier 3", False, 2),
    ("Wadi ad-Dawasir", 1, 20.4920, 44.7810, "Tier 3", False, 2),
    ("Az Zulfi", 1, 26.2994, 44.8155, "Tier 3", False, 1),
    ("Shaqra", 1, 25.2484, 45.2516, "Tier 3", False, 1),
    ("Jeddah", 2, 21.4858, 39.1925, "Tier 1", True, 42),
    ("Makkah", 2, 21.3891, 39.8579, "Tier 1", False, 26),
    ("Taif", 2, 21.2703, 40.4158, "Tier 2", False, 8),
    ("Rabigh", 2, 22.7986, 39.0349, "Tier 3", True, 2),
    ("Al Qunfudhah", 2, 19.1264, 41.0789, "Tier 3", True, 2),
    ("Al Lith", 2, 20.1500, 40.2667, "Tier 3", True, 1),
    ("Dammam", 3, 26.4207, 50.0888, "Tier 1", True, 20),
    ("Al Khobar", 3, 26.2172, 50.1971, "Tier 1", True, 12),
    ("Dhahran", 3, 26.2361, 50.0393, "Tier 2", True, 5),
    ("Al Hofuf", 3, 25.3833, 49.5833, "Tier 2", False, 8),
    ("Jubail", 3, 27.0046, 49.6460, "Tier 2", True, 6),
    ("Hafar Al-Batin", 3, 28.4328, 45.9708, "Tier 2", False, 4),
    ("Qatif", 3, 26.5196, 50.0115, "Tier 2", True, 4),
    ("Ras Tanura", 3, 26.6440, 50.1590, "Tier 3", True, 2),
    ("Khafji", 3, 28.4391, 48.4913, "Tier 3", True, 1),
    ("Madinah", 4, 24.5247, 39.5692, "Tier 1", False, 16),
    ("Yanbu", 4, 24.0890, 38.0618, "Tier 2", True, 5),
    ("AlUla", 4, 26.6085, 37.9232, "Tier 3", False, 3),
    ("Badr", 4, 23.7800, 38.7906, "Tier 3", False, 1),
    ("Abha", 5, 18.2164, 42.5053, "Tier 2", False, 7),
    ("Khamis Mushait", 5, 18.3000, 42.7333, "Tier 2", False, 6),
    ("Bisha", 5, 20.0005, 42.6052, "Tier 3", False, 2),
    ("Al Namas", 5, 19.1167, 42.1333, "Tier 3", False, 1),
    ("Buraidah", 6, 26.3260, 43.9750, "Tier 2", False, 8),
    ("Unaizah", 6, 26.0843, 43.9935, "Tier 2", False, 4),
    ("Ar Rass", 6, 25.8694, 43.4973, "Tier 3", False, 2),
    ("Tabuk", 7, 28.3835, 36.5662, "Tier 2", False, 6),
    ("NEOM Gulf of Aqaba", 7, 28.0350, 34.8000, "Tier 3", True, 4),
    ("Duba", 7, 27.3510, 35.6960, "Tier 3", True, 1),
    ("Umluj", 7, 25.0213, 37.2685, "Tier 3", True, 2),
    ("Hail", 8, 27.5114, 41.7208, "Tier 2", False, 5),
    ("Baqaa", 8, 27.8833, 42.4167, "Tier 3", False, 1),
    ("Jazan", 9, 16.8892, 42.5511, "Tier 2", True, 5),
    ("Sabya", 9, 17.1495, 42.6254, "Tier 3", False, 2),
    ("Abu Arish", 9, 16.9689, 42.8325, "Tier 3", False, 2),
    ("Najran", 10, 17.5656, 44.2289, "Tier 2", False, 4),
    ("Sharurah", 10, 17.4667, 47.1167, "Tier 3", False, 1),
    ("Al Bahah", 11, 20.0129, 41.4677, "Tier 3", False, 2),
    ("Baljurashi", 11, 19.8590, 41.5590, "Tier 3", False, 1),
    ("Sakaka", 12, 29.9697, 40.2064, "Tier 3", False, 3),
    ("Dumat al-Jandal", 12, 29.8118, 39.8687, "Tier 3", False, 1),
    ("Qurayyat", 12, 31.3318, 37.3428, "Tier 3", False, 1),
    ("Arar", 13, 30.9753, 41.0381, "Tier 3", False, 3),
    ("Rafha", 13, 29.6202, 43.4948, "Tier 3", False, 1),
    ("Turaif", 13, 31.6725, 38.6637, "Tier 3", False, 1),
]

TENANTS = [
    # id, name, type, group, rating, msa date, anchor_share
    ("TEN-STC", "stc", "MNO", "Saudi Telecom Company", "A+", "2017-05-01", 46),
    ("TEN-MBY", "Mobily", "MNO", "Etihad Etisalat", "BBB+", "2018-02-15", 24),
    ("TEN-ZAIN", "Zain KSA", "MNO", "Zain Group", "BBB", "2018-07-01", 18),
    ("TEN-SLM", "Salam", "ISP", "Integrated Telecom Co", "BBB-", "2020-03-10", 3),
    ("TEN-VMK", "Virgin Mobile KSA", "MVNO", "Virgin Mobile Middle East", "BB+", "2019-11-01", 1),
    ("TEN-LEB", "Lebara KSA", "MVNO", "Lebara Group", "BB", "2019-08-20", 1),
    ("TEN-GOT", "GO Telecom", "ISP", "Etihad Atheeb", "BB-", "2021-01-12", 1),
    ("TEN-RSG", "Red Sea Global", "Enterprise", "PIF", "A", "2022-06-01", 2),
    ("TEN-NEOM", "NEOM Tech & Digital", "Enterprise", "PIF", "A", "2022-09-15", 2),
    ("TEN-ARMC", "Saudi Aramco Digital", "Enterprise", "Saudi Aramco", "A+", "2023-02-01", 1),
    ("TEN-MOI", "Ministry of Interior (TETRA)", "Government", "Government of KSA", "A+", "2016-01-01", 1),
    ("TEN-SEC", "Saudi Electricity Co (Smart Grid)", "Utility", "SEC", "A", "2024-04-01", 1),
]
TENANT_IDS = [t[0] for t in TENANTS]
TENANT_W = [t[6] for t in TENANTS]
MNO_IDS = ["TEN-STC", "TEN-MBY", "TEN-ZAIN"]

VENDORS = [
    # id, name, type, country, local_content
    ("V001", "Huawei", "RAN", "China", False),
    ("V002", "Ericsson", "RAN", "Sweden", False),
    ("V003", "Nokia", "RAN", "Finland", False),
    ("V004", "ZTE", "RAN", "China", False),
    ("V005", "CommScope", "Antenna", "USA", False),
    ("V006", "Delta Electronics", "Power", "Taiwan", False),
    ("V007", "Vertiv", "Power", "USA", False),
    ("V008", "Eaton", "Power", "Ireland", False),
    ("V009", "Caterpillar", "Generator", "USA", False),
    ("V010", "Cummins", "Generator", "USA", False),
    ("V011", "Al-Babtain Power & Telecom", "Tower Fabrication", "Saudi Arabia", True),
    ("V012", "Valmont", "Tower Fabrication", "USA", False),
    ("V013", "Saudi Cable Company", "Fiber", "Saudi Arabia", True),
    ("V014", "Zamil Air Conditioners", "Cooling", "Saudi Arabia", True),
    ("V015", "Nesma Telecom & Technology", "Field Services", "Saudi Arabia", True),
    ("V016", "Al Fozan Telecom Services", "Field Services", "Saudi Arabia", True),
    ("V017", "Jinko Solar", "Solar", "China", False),
]

FIRST = ["Abdullah", "Mohammed", "Fahad", "Saud", "Khalid", "Faisal", "Turki", "Nasser", "Omar", "Saad",
         "Sultan", "Majed", "Bandar", "Nawaf", "Yousef", "Hamad", "Ali", "Ibrahim", "Noura", "Reem", "Sara", "Lama"]
FAMILY = ["Al-Qahtani", "Al-Otaibi", "Al-Ghamdi", "Al-Zahrani", "Al-Dosari", "Al-Shammari", "Al-Harbi",
          "Al-Mutairi", "Al-Anazi", "Al-Shehri", "Al-Subaie", "Al-Rashid", "Al-Juhani", "Al-Malki", "Al-Rajhi"]
COMPANIES = ["Real Estate Co", "Holding Group", "Trading Est", "Development Co", "Investment Co", "Properties"]


def gen_reference():
    regions = [{"region_id": r[0], "region_code": r[1], "region_name": r[2], "region_name_ar": r[3],
                "population": r[4], "area_km2": r[5], "country_code": "SA"} for r in REGIONS]
    cities = []
    for i, c in enumerate(CITIES, start=1):
        cities.append({"city_id": 100 + i, "city_name": c[0], "region_id": c[1],
                       "latitude": c[2], "longitude": c[3], "city_tier": c[4], "is_coastal": c[5]})
    tenants = [{"tenant_id": t[0], "tenant_name": t[1], "tenant_type": t[2], "parent_group": t[3],
                "credit_rating": t[4], "msa_signed_date": t[5], "billing_currency": "SAR",
                "account_manager": f"{rng.choice(FIRST)} {rng.choice(FAMILY)}"} for t in TENANTS]
    vendors = [{"vendor_id": v[0], "vendor_name": v[1], "vendor_type": v[2], "hq_country": v[3],
                "is_local_content": v[4], "vendor_rating": round(rng.uniform(3.1, 4.9), 1)} for v in VENDORS]
    landlords = []
    for i in range(1, 261):
        kind = weighted([("Individual", 55), ("Company", 25), ("Government", 10), ("Municipality", 10)])
        if kind == "Individual":
            name = f"{rng.choice(FIRST)} {rng.choice(FAMILY)}"
        elif kind == "Company":
            name = f"{rng.choice(FAMILY).replace('Al-', 'Al ')} {rng.choice(COMPANIES)}"
        elif kind == "Government":
            name = rng.choice(["Ministry of Municipal & Rural Affairs", "General Authority of Awqaf",
                               "Saudi Ports Authority", "Ministry of Education", "Royal Commission"])
        else:
            name = f"{rng.choice([c[0] for c in CITIES])} Municipality"
        landlords.append({"landlord_id": f"LL-{i:05d}", "landlord_name": name, "landlord_type": kind,
                          "national_id_masked": f"1{rng.randint(0, 99):02d}*****{rng.randint(100, 999)}",
                          "iban_masked": f"SA{rng.randint(10, 99)}**************{rng.randint(1000, 9999)}",
                          "phone": f"+9665{rng.randint(0, 9)}{rng.randint(1000000, 9999999)}"})
    return regions, cities, tenants, vendors, landlords


# ─── Operational data ────────────────────────────────────────────────────────
DISTRICTS = ["North", "South", "East", "West", "Central", "Industrial", "Airport Rd", "Corniche",
             "Ring Rd", "Old Town", "Olaya", "University", "Highway", "Port", "Hills", "Valley"]

STRUCT = {
    # structure: (height range, slot range, capacity kg)
    "Lattice": ((36, 72), (4, 6), (900, 1800)),
    "Monopole": ((20, 45), (3, 3), (500, 900)),
    "Guyed Mast": ((45, 90), (3, 4), (600, 1100)),
    "Camouflaged Palm": ((18, 30), (2, 2), (300, 500)),
    "Rooftop Pole": ((6, 15), (2, 3), (200, 400)),
    "Small Cell Pole": ((8, 12), (1, 1), (80, 150)),
}


def gen_ops(cities, landlords):
    city_weights = [c[6] for c in CITIES]
    city_rows = cities
    teams = []
    for r in REGIONS:
        for n in (1, 2):
            teams.append({"team_id": f"FT-{r[1]}-{n:02d}", "team_name": f"{r[2]} Field Team {n}",
                          "region_id": r[0], "contractor_id": rng.choice(["V015", "V016"]),
                          "team_lead": f"{rng.choice(FIRST)} {rng.choice(FAMILY)}",
                          "headcount": rng.randint(4, 14), "shift_pattern": rng.choice(["Day", "Day/Night", "24x7"]),
                          "is_active": True})
    teams_by_region = {}
    for t in teams:
        teams_by_region.setdefault(t["region_id"], []).append(t["team_id"])

    sites, towers, slots = [], [], []
    seq_by_region: dict[int, int] = {}
    for _ in range(320):
        ci = rng.choices(range(len(CITIES)), weights=city_weights, k=1)[0]
        c = CITIES[ci]
        city = city_rows[ci]
        rid = c[1]
        seq_by_region[rid] = seq_by_region.get(rid, 0) + 1
        sid = f"TWL-{REGION_CODE[rid]}-{seq_by_region[rid]:04d}"
        urban = c[4] == "Tier 1"
        stype = weighted([("Greenfield", 30 if urban else 75), ("Rooftop", 45 if urban else 12),
                          ("Small Cell", 14 if urban else 3), ("In-Building", 8 if urban else 2), ("COW", 3)])
        status = weighted([("On-Air", 88), ("Under Construction", 6), ("Planned", 3), ("Decommissioned", 3)])
        power = weighted([("Grid", 55 if urban else 25), ("Grid+DG", 25), ("DG Only", 5 if urban else 30),
                          ("Hybrid Solar-DG", 10), ("Solar", 3)])
        comm = rand_date(date(2006, 1, 1), date(2025, 12, 31)) if status != "Planned" else None
        sites.append({
            "site_id": sid, "site_name": f"{c[0]} {rng.choice(DISTRICTS)} {seq_by_region[rid]:03d}",
            "city_id": city["city_id"], "landlord_id": rng.choice(landlords)["landlord_id"],
            "site_type": stype, "site_status": status,
            "latitude": round(c[2] + rng.uniform(-0.18, 0.18), 6), "longitude": round(c[3] + rng.uniform(-0.18, 0.18), 6),
            "elevation_m": rng.randint(0, 2400) if rid in (5, 11) else rng.randint(0, 900),
            "power_source": power, "grid_connection_kva": (rng.choice([25, 50, 63, 100]) if "Grid" in power else 0),
            "commissioned_date": comm.isoformat() if comm else "", "ground_area_sqm": rng.randint(40, 900) if stype == "Greenfield" else rng.randint(6, 80),
            "access_type": weighted([("24x7", 60), ("Restricted Hours", 30), ("Escort Required", 10)]),
            "is_hub_site": rng.random() < 0.12, "cluster_code": f"CL-{REGION_CODE[rid]}-{rng.randint(1, 9):02d}",
        })
        # structures
        if stype == "Greenfield":
            structs = [weighted([("Lattice", 45), ("Monopole", 30), ("Guyed Mast", 15), ("Camouflaged Palm", 10)])]
            if rng.random() < 0.05:
                structs.append("Monopole")
        elif stype == "Rooftop":
            structs = ["Rooftop Pole"] * rng.choice([1, 1, 2])
        elif stype == "Small Cell":
            structs = ["Small Cell Pole"]
        elif stype == "COW":
            structs = ["Monopole"]
        else:
            structs = []  # in-building: no external structure
        for s in structs:
            hr, sr, cap = STRUCT[s]
            tid = f"TWR-{len(towers) + 1:06d}"
            nslots = rng.randint(*sr)
            towers.append({
                "tower_id": tid, "site_id": sid, "structure_type": s,
                "height_m": round(rng.uniform(*hr), 1), "max_tenant_slots": nslots,
                "structural_capacity_kg": rng.randint(*cap),
                "fabricator_vendor_id": rng.choice(["V011", "V011", "V012"]),
                "build_year": (comm.year if comm else 2026),
                "last_inspection_date": rand_date(date(2025, 1, 1), date(2026, 9, 30)).isoformat() if comm else "",
                "structural_status": weighted([("Good", 80), ("Monitor", 15), ("Needs Reinforcement", 5)]),
            })
            for k in range(1, nslots + 1):
                slots.append({"tower_id": tid, "slot_no": k,
                              "mount_height_m": round(towers[-1]["height_m"] - (k - 1) * rng.uniform(2.5, 4.5), 1),
                              "azimuth_set": rng.choice(["0/120/240", "30/150/270", "60/180/300", "90/210/330"]),
                              "max_load_kg": rng.randint(120, 400), "slot_status": "Available"})

    site_by_id = {s["site_id"]: s for s in sites}
    onair = [s for s in sites if s["site_status"] == "On-Air"]

    # leases (tenancies) on tower slots of on-air sites
    leases = []
    slots_by_tower: dict[str, list] = {}
    for sl in slots:
        slots_by_tower.setdefault(sl["tower_id"], []).append(sl)
    for t in towers:
        s = site_by_id[t["site_id"]]
        if s["site_status"] != "On-Air":
            continue
        tslots = slots_by_tower[t["tower_id"]]
        n = min(len(tslots), weighted([(1, 35), (2, 40), (3, 18), (4, 7)]))
        used_tenants: set[str] = set()
        for k in range(n):
            sl = tslots[k]
            tenant = rng.choices(MNO_IDS, weights=[50, 28, 22])[0] if k == 0 else rng.choices(TENANT_IDS, weights=TENANT_W)[0]
            if tenant in used_tenants:
                tenant = rng.choice([x for x in TENANT_IDS if x not in used_tenants])
            used_tenants.add(tenant)
            start = rand_date(date(2017, 1, 1), date(2026, 6, 30))
            if s["commissioned_date"] and start.isoformat() < s["commissioned_date"]:
                start = date.fromisoformat(s["commissioned_date"])
            term_years = rng.choice([5, 10, 10, 15])
            end = date(min(start.year + term_years, 2045), start.month, min(start.day, 28))
            status = "Active" if end >= date(2026, 9, 30) else "Expired"
            if rng.random() < 0.03:
                status = "Pending"
            fee = {"Lattice": 9500, "Monopole": 7800, "Guyed Mast": 8200, "Camouflaged Palm": 8800,
                   "Rooftop Pole": 6200, "Small Cell Pole": 2600}[t["structure_type"]]
            fee *= (1.0 if k == 0 else 0.82) * rng.uniform(0.85, 1.2)
            sl["slot_status"] = "Occupied" if status == "Active" else "Reserved"
            leases.append({
                "lease_id": f"LSE-{len(leases) + 1:06d}", "site_id": s["site_id"], "tower_id": t["tower_id"],
                "slot_no": sl["slot_no"], "tenant_id": tenant,
                "lease_start": start.isoformat(), "lease_end": end.isoformat(),
                "monthly_fee_sar": round(fee, 2), "annual_escalation_pct": rng.choice([0.0, 1.5, 2.0, 2.5, 3.0]),
                "technology": weighted([("2G/3G/4G", 25), ("4G/5G", 45), ("5G NR", 20), ("IoT/NB-IoT", 5), ("TETRA", 5)]),
                "lease_status": status, "is_anchor_tenant": k == 0, "power_included": rng.random() < 0.7,
            })
    # two deliberately bad lease records (end before start) for silver expectations
    for l in rng.sample(leases, 2):
        l["lease_end"] = (date.fromisoformat(l["lease_start"]) - timedelta(days=30)).isoformat()

    # monthly lease invoices (JSON lines)
    invoices = []
    for l in leases:
        if l["lease_status"] not in ("Active", "Expired"):
            continue
        for m in BILL_MONTHS:
            if not (l["lease_start"] <= m.isoformat() <= l["lease_end"]):
                continue
            amt = round(l["monthly_fee_sar"], 2)
            issued = m + timedelta(days=rng.randint(0, 3))
            due = issued + timedelta(days=30)
            st = weighted([("Paid", 84), ("Open", 8), ("Overdue", 6), ("Disputed", 2)])
            if m >= date(2026, 9, 1) and st == "Paid" and rng.random() < 0.6:
                st = "Open"
            paid = (due - timedelta(days=rng.randint(-20, 25))).isoformat() if st == "Paid" else None
            invoices.append({"invoice_id": f"INV-{m.strftime('%y%m')}-{len(invoices) + 1:06d}", "lease_id": l["lease_id"],
                             "tenant_id": l["tenant_id"], "billing_month": m.isoformat(), "amount_sar": amt,
                             "vat_sar": round(amt * 0.15, 2), "invoice_status": st, "issued_date": issued.isoformat(),
                             "due_date": due.isoformat(), "paid_date": paid})

    # ground leases (Tawal → landlord)
    ground = []
    for s in sites:
        if s["site_status"] == "Planned":
            continue
        start = date.fromisoformat(s["commissioned_date"]) if s["commissioned_date"] else date(2026, 1, 1)
        ground.append({"ground_lease_id": f"GL-{len(ground) + 1:05d}", "site_id": s["site_id"],
                       "landlord_id": s["landlord_id"],
                       "annual_rent_sar": round(rng.uniform(12000, 85000) if s["site_type"] == "Greenfield" else rng.uniform(6000, 42000), 2),
                       "start_date": start.isoformat(), "end_date": date(start.year + 10, start.month, min(start.day, 28)).isoformat(),
                       "payment_frequency": weighted([("Annual", 70), ("Semi-Annual", 20), ("Quarterly", 10)]),
                       "has_renewal_option": rng.random() < 0.8})

    # equipment
    equipment = []
    towers_by_site: dict[str, list] = {}
    for t in towers:
        towers_by_site.setdefault(t["site_id"], []).append(t)
    leases_by_tower: dict[str, list] = {}
    for l in leases:
        leases_by_tower.setdefault(l["tower_id"], []).append(l)
    RAN_V = {"TEN-STC": ["V001", "V002"], "TEN-MBY": ["V001", "V003"], "TEN-ZAIN": ["V001", "V004"]}
    MODELS = {"Antenna": ["CommScope SBNHH-1D65C", "Huawei AAU5613", "Ericsson AIR 6449"],
              "RRU": ["Huawei RRU5502", "Ericsson Radio 4415", "Nokia AirScale RRH"],
              "BBU": ["Huawei BBU5900", "Ericsson Baseband 6630", "Nokia AirScale System Module"],
              "Microwave Dish": ["Huawei OptiX RTN 980", "Ericsson MINI-LINK 6352", "Nokia Wavence"]}

    def add_eq(site, tower_id, lease_id, etype, vendor, model, ownership, kw, kg, inst):
        equipment.append({
            "equipment_id": f"EQ-{len(equipment) + 1:07d}", "site_id": site["site_id"], "tower_id": tower_id,
            "lease_id": lease_id, "vendor_id": vendor, "equipment_type": etype, "model": model,
            "serial_number": f"SN{rng.randint(10**9, 10**10 - 1)}", "ownership": ownership,
            "install_date": inst.isoformat(), "warranty_end": (inst + timedelta(days=365 * rng.choice([3, 5]))).isoformat(),
            "equipment_status": weighted([("In Service", 90), ("Faulty", 4), ("Spare", 3), ("Decommissioned", 3)]),
            "power_kw": kw, "weight_kg": kg,
        })

    for s in onair:
        inst0 = date.fromisoformat(s["commissioned_date"]) if s["commissioned_date"] else date(2020, 1, 1)
        for t in towers_by_site.get(s["site_id"], []):
            for l in leases_by_tower.get(t["tower_id"], []):
                if l["lease_status"] != "Active":
                    continue
                vend = RAN_V.get(l["tenant_id"], ["V001", "V002", "V003"])
                inst = max(inst0, date.fromisoformat(l["lease_start"]))
                for _ in range(rng.choice([3, 3, 6])):
                    add_eq(s, t["tower_id"], l["lease_id"], "Antenna", "V005" if rng.random() < 0.4 else rng.choice(vend),
                           rng.choice(MODELS["Antenna"]), "Tenant", 0.0, round(rng.uniform(18, 45), 1), inst)
                for _ in range(rng.choice([3, 3, 6])):
                    add_eq(s, t["tower_id"], l["lease_id"], "RRU", rng.choice(vend), rng.choice(MODELS["RRU"]), "Tenant",
                           round(rng.uniform(0.3, 0.9), 2), round(rng.uniform(15, 30), 1), inst)
                add_eq(s, None, l["lease_id"], "BBU", rng.choice(vend), rng.choice(MODELS["BBU"]), "Tenant",
                       round(rng.uniform(0.2, 0.5), 2), round(rng.uniform(8, 15), 1), inst)
                if rng.random() < 0.55:
                    add_eq(s, t["tower_id"], l["lease_id"], "Microwave Dish", rng.choice(vend), rng.choice(MODELS["Microwave Dish"]),
                           "Tenant", round(rng.uniform(0.05, 0.12), 2), round(rng.uniform(20, 60), 1), inst)
        # Tawal-owned passive infra
        add_eq(s, None, None, "Rectifier", rng.choice(["V006", "V007", "V008"]), "48V DC Power System", "Tawal",
               round(rng.uniform(3, 9), 1), round(rng.uniform(80, 160), 1), inst0)
        add_eq(s, None, None, "Battery Bank", rng.choice(["V006", "V007"]), "Li-ion 48V 200Ah", "Tawal", 0.0,
               round(rng.uniform(90, 260), 1), inst0)
        if "DG" in s["power_source"]:
            add_eq(s, None, None, "Diesel Generator", rng.choice(["V009", "V010"]), rng.choice(["DG 20kVA", "DG 30kVA", "DG 60kVA"]),
                   "Tawal", 0.0, round(rng.uniform(450, 1200), 1), inst0)
            add_eq(s, None, None, "Fuel Tank", "V010", "1000L Double-Wall Tank", "Tawal", 0.0, 380.0, inst0)
        if "Solar" in s["power_source"]:
            add_eq(s, None, None, "Solar Array", "V017", "Tiger Neo 6kWp Array", "Tawal", 0.0, round(rng.uniform(300, 700), 1), inst0)
        if s["site_type"] in ("Greenfield", "COW"):
            add_eq(s, None, None, "Air Conditioner", "V014", "Zamil Shelter AC 2TR", "Tawal", round(rng.uniform(2.2, 3.5), 1), 95.0, inst0)

    # fiber / microwave links between nearby on-air sites
    links = []
    by_city: dict[int, list] = {}
    for s in onair:
        by_city.setdefault(s["city_id"], []).append(s)
    for group in by_city.values():
        hubs = [s for s in group if s["is_hub_site"]] or group[:1]
        for s in group:
            if s in hubs and len(group) > 1:
                continue
            hub = rng.choice(hubs)
            if hub["site_id"] == s["site_id"]:
                continue
            lt = weighted([("Fiber", 55), ("Microwave", 35), ("E-Band", 10)])
            links.append({"link_id": f"LNK-{len(links) + 1:05d}", "a_site_id": hub["site_id"], "z_site_id": s["site_id"],
                          "link_type": lt, "capacity_gbps": {"Fiber": rng.choice([10.0, 40.0, 100.0]), "Microwave": rng.choice([0.5, 1.0, 2.0]),
                                                              "E-Band": rng.choice([5.0, 10.0])}[lt],
                          "distance_km": round(rng.uniform(0.4, 28.0), 2),
                          "owner_tenant_id": rng.choice([None, None, "TEN-STC", "TEN-MBY", "TEN-ZAIN"]),
                          "commissioned_date": rand_date(date(2012, 1, 1), date(2026, 6, 30)).isoformat(),
                          "link_status": weighted([("Up", 93), ("Degraded", 5), ("Down", 2)])})

    # alarms (NOC events) over the ops window — JSON lines, with ~1% exact duplicates
    eq_by_site: dict[str, list] = {}
    for e in equipment:
        eq_by_site.setdefault(e["site_id"], []).append(e)
    ALARM_CODES = [("PWR-001", "Mains Failure", "Major", "Rectifier"), ("PWR-002", "Low Battery Voltage", "Critical", "Battery Bank"),
                   ("GEN-003", "Generator Low Fuel", "Major", "Fuel Tank"), ("GEN-004", "Generator Fail to Start", "Critical", "Diesel Generator"),
                   ("ENV-005", "High Shelter Temperature", "Minor", "Air Conditioner"), ("SEC-006", "Shelter Door Open", "Warning", None),
                   ("TX-007", "Transmission Link Down", "Critical", "Microwave Dish"), ("RF-008", "VSWR Threshold Crossed", "Minor", "Antenna"),
                   ("RF-009", "RRU Out of Service", "Major", "RRU")]
    alarms = []
    span_h = int((OPS_END - OPS_START).days * 24)
    for _ in range(7000):
        s = rng.choice(onair)
        code, text, sev, etype = rng.choice(ALARM_CODES)
        cands = [e for e in eq_by_site.get(s["site_id"], []) if e["equipment_type"] == etype] if etype else []
        if etype and not cands:
            code, text, sev, etype = ALARM_CODES[5]
        eq = rng.choice(cands)["equipment_id"] if cands else None
        raised = datetime.combine(OPS_START, datetime.min.time()) + timedelta(hours=rng.randint(0, span_h), minutes=rng.randint(0, 59))
        cleared = raised + timedelta(minutes=rng.randint(3, 60 * 30)) if rng.random() < 0.96 else None
        alarms.append({"alarm_id": f"ALM-{len(alarms) + 1:07d}", "site_id": s["site_id"], "equipment_id": eq,
                       "alarm_code": code, "alarm_text": text, "severity": sev if rng.random() > 0.03 else sev.upper(),
                       "raised_at": ts(raised), "cleared_at": ts(cleared) if cleared else None,
                       "source_system": rng.choice(["NOC-U2020", "NOC-NetAct", "NOC-ENM", "BMS"])})
    alarms_out = alarms + [dict(a) for a in rng.sample(alarms, 70)]  # exact duplicates
    rng.shuffle(alarms_out)

    # work orders — corrective ones are raised from alarms
    rid_of_code = {r[1]: r[0] for r in REGIONS}
    team_of_site = {s["site_id"]: rng.choice(teams_by_region[rid_of_code[s["site_id"].split("-")[1]]])
                    for s in sites}
    wos = []
    SLA = {"P1": 4, "P2": 8, "P3": 24, "P4": 72}

    def add_wo(site_id, eq_id, alarm_id, wtype, prio, opened):
        closed = opened + timedelta(hours=rng.uniform(0.5, SLA[prio] * 2.2)) if rng.random() < 0.92 else None
        st = "Closed" if closed else weighted([("Open", 50), ("In Progress", 45), ("Cancelled", 5)])
        sla_met = (closed - opened).total_seconds() / 3600 <= SLA[prio] if closed else None
        wos.append({"work_order_id": f"WO-{opened.strftime('%y%m')}-{len(wos) + 1:06d}", "site_id": site_id,
                    "equipment_id": eq_id, "alarm_id": alarm_id, "team_id": team_of_site[site_id],
                    "wo_type": wtype, "priority": prio, "opened_at": ts(opened), "closed_at": ts(closed) if closed else None,
                    "wo_status": st, "sla_hours": SLA[prio], "sla_met": sla_met,
                    "labor_hours": round(rng.uniform(0.5, 9.0), 1) if closed else None,
                    "cost_sar": round(rng.uniform(250, 6800), 2) if closed else None})

    for a in alarms:
        if a["severity"].title() in ("Critical", "Major") and rng.random() < 0.3:
            opened = datetime.strptime(a["raised_at"], "%Y-%m-%d %H:%M:%S") + timedelta(minutes=rng.randint(2, 45))
            add_wo(a["site_id"], a["equipment_id"], a["alarm_id"], "Corrective Maintenance",
                   "P1" if a["severity"].title() == "Critical" else "P2", opened)
    for s in onair:
        for _ in range(rng.randint(1, 4)):
            opened = datetime.combine(rand_date(OPS_START, OPS_END), datetime.min.time()) + timedelta(hours=rng.randint(6, 18))
            wtype = weighted([("Preventive Maintenance", 55), ("Refueling", 20 if "DG" in s["power_source"] else 0),
                              ("Site Audit", 12), ("Tenant Upgrade", 8), ("Installation", 5)])
            add_wo(s["site_id"], None, None, wtype, weighted([("P3", 60), ("P4", 40)]), opened)

    # daily energy readings per on-air site (split into monthly files), with
    # ~0.4% negative grid readings and ~0.8% duplicates for silver to clean
    energy = {7: [], 8: [], 9: []}
    rid = 0
    for s in onair:
        base = rng.uniform(55, 160) if s["site_type"] in ("Greenfield", "COW") else rng.uniform(12, 60)
        grid = "Grid" in s["power_source"]
        dg = "DG" in s["power_source"]
        solar = "Solar" in s["power_source"]
        d = OPS_START
        while d <= OPS_END:
            rid += 1
            heat = 1.0 + (0.18 if d.month in (7, 8) else 0.08)
            total = base * heat * rng.uniform(0.9, 1.1)
            sol = round(total * rng.uniform(0.25, 0.45), 2) if solar else 0.0
            dgk = round((total - sol) * (rng.uniform(0.05, 0.25) if grid else 1.0), 2) if dg else 0.0
            grk = round(max(total - sol - dgk, 0), 2) if grid else 0.0
            row = {"reading_id": rid, "site_id": s["site_id"], "reading_date": d.isoformat(),
                   "grid_kwh": grk if rng.random() > 0.004 else -round(rng.uniform(1, 50), 2),
                   "dg_kwh": dgk, "solar_kwh": sol,
                   "dg_runtime_hours": round(dgk / rng.uniform(9, 14), 2) if dg else 0.0,
                   "fuel_level_pct": round(rng.uniform(18, 98), 1) if dg else "",
                   "battery_soc_pct": round(rng.uniform(62, 100), 1),
                   "avg_temp_c": round(rng.uniform(31, 47) if d.month in (7, 8) else rng.uniform(27, 41), 1),
                   "power_availability_pct": round(min(100.0, rng.gauss(99.6, 0.6)), 2)}
            energy[d.month].append(row)
            if rng.random() < 0.008:
                energy[d.month].append(dict(row))
            d += timedelta(days=1)

    # utility (SEC) bills for grid-connected sites
    bills = []
    for s in onair:
        if "Grid" not in s["power_source"]:
            continue
        meter = f"SEC-{rng.randint(10**7, 10**8 - 1)}"
        for m in (7, 8, 9):
            kwh = round(sum(r["grid_kwh"] for r in energy[m] if r["site_id"] == s["site_id"] and r["grid_kwh"] > 0), 1)
            tariff = round(rng.choice([0.18, 0.20, 0.30, 0.32]), 4)
            bills.append({"bill_id": f"SECB-2026{m:02d}-{len(bills) + 1:05d}", "site_id": s["site_id"],
                          "billing_month": date(2026, m, 1).isoformat(), "meter_no": meter, "kwh_billed": kwh,
                          "tariff_sar_per_kwh": tariff, "amount_sar": round(kwh * tariff, 2),
                          "bill_status": weighted([("Paid", 85), ("Unpaid", 12), ("Disputed", 3)])})

    # inconsistent casing / whitespace in a few site_type values (silver trims + initcaps)
    for s in rng.sample(sites, 9):
        s["site_type"] = rng.choice([s["site_type"].upper(), s["site_type"].lower() + " ", " " + s["site_type"]])

    return (teams, sites, towers, slots, leases, invoices, ground, equipment, links,
            alarms_out, wos, energy, bills)


def main():
    regions, cities, tenants, vendors, landlords = gen_reference()
    (teams, sites, towers, slots, leases, invoices, ground, equipment, links,
     alarms, wos, energy, bills) = gen_ops(cities, landlords)

    write_csv("regions", regions)
    write_csv("cities", cities)
    write_csv("tenants", tenants)
    write_csv("vendors", vendors)
    write_csv("landlords", landlords)
    write_csv("field_teams", teams)
    write_csv("sites", sites)
    write_csv("towers", towers)
    write_csv("tower_slots", slots)
    write_csv("leases", leases)
    write_jsonl("lease_invoices", invoices)
    write_csv("ground_leases", ground)
    write_csv("equipment", equipment)
    write_csv("fiber_links", links)
    write_jsonl("alarms", alarms)
    write_jsonl("work_orders", wos)
    for m, rows in energy.items():
        write_csv("energy_readings", rows, part=f"2026_{m:02d}")
    write_csv("utility_bills", bills)

    counts = {"regions": len(regions), "cities": len(cities), "tenants": len(tenants), "vendors": len(vendors),
              "landlords": len(landlords), "field_teams": len(teams), "sites": len(sites), "towers": len(towers),
              "tower_slots": len(slots), "leases": len(leases), "lease_invoices": len(invoices),
              "ground_leases": len(ground), "equipment": len(equipment), "fiber_links": len(links),
              "alarms": len(alarms), "work_orders": len(wos),
              "energy_readings": sum(len(v) for v in energy.values()), "utility_bills": len(bills)}
    for k, v in counts.items():
        print(f"{k:16s} {v:>7,d}")


if __name__ == "__main__":
    main()
