"""
Generate pre-cached CSV datasets for City Pulse.
- algorithmic_tools: downloaded from NYC Open Data (real data)
- hmda_nyc: downloaded from CFPB HMDA Historic Data (real data, NY 2017, filtered to NYC)
- delivery_workers: representative synthetic data based on DCWP study findings
- ppp_nyc: representative synthetic data based on SBA PPP structure
"""
import csv
import io
import os
import random
import urllib.request
import zipfile
import openpyxl

OUTDIR = os.path.join(os.path.dirname(__file__), "..", "mcp_server", "data")
os.makedirs(OUTDIR, exist_ok=True)

random.seed(42)

# ---------------------------------------------------------------------------
# 1. Algorithmic Tools — real data from NYC Open Data
# ---------------------------------------------------------------------------
def download_algorithmic_tools():
    url = "https://data.cityofnewyork.us/resource/jaw4-yuem.json?$limit=500"
    path = os.path.join(OUTDIR, "algorithmic_tools.csv")
    print("Downloading algorithmic_tools from NYC Open Data...")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "CityPulse/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            import json
            rows = json.load(resp)
        if not rows:
            raise ValueError("Empty response")
        cols = list(rows[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        print(f"  → {len(rows)} rows written to algorithmic_tools.csv")
    except Exception as e:
        print(f"  Download failed ({e}), generating synthetic algorithmic_tools...")
        _synthetic_algorithmic_tools(path)


def _synthetic_algorithmic_tools(path):
    agencies = [
        "NYPD", "ACS", "DOE", "HRA", "DOHMH", "DOF", "DCAS",
        "HPD", "DSNY", "DEP", "DOT", "SBS", "DYCD", "DOC", "DHS",
    ]
    purposes = [
        "Predictive Policing", "Child Welfare Screening", "Benefits Eligibility",
        "Risk Assessment", "Fraud Detection", "Resource Allocation",
        "School Placement", "Housing Inspection Prioritization",
        "Recidivism Prediction", "Loan Underwriting Support",
    ]
    vendors = ["Palantir", "SAS", "IBM", "Microsoft", "Accenture", "In-house", "Deloitte", "Booz Allen"]
    statuses = ["Active", "Active", "Active", "Active", "Pilot", "Decommissioned"]

    rows = []
    tool_id = 1
    for agency in agencies:
        num_tools = random.randint(1, 4)
        for _ in range(num_tools):
            rows.append({
                "tool_id": f"AT-{tool_id:04d}",
                "agency": agency,
                "tool_name": f"{agency} {random.choice(purposes)} System",
                "purpose": random.choice(purposes),
                "vendor": random.choice(vendors),
                "status": random.choice(statuses),
                "procurement_year": random.randint(2015, 2023),
                "data_sources": "Internal records, third-party databases",
                "impact_population": random.choice([
                    "Residents", "Children", "Job Seekers", "Tenants", "Arrestees"
                ]),
                "audit_completed": random.choice(["Yes", "No", "Pending"]),
            })
            tool_id += 1

    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  → {len(rows)} synthetic rows written to algorithmic_tools.csv")


# ---------------------------------------------------------------------------
# 2. HMDA NYC — real data from CFPB Historic HMDA (NY 2017, NYC counties only)
# ---------------------------------------------------------------------------
# NYC county codes within NY state
NYC_COUNTIES = {"005": "Bronx", "047": "Brooklyn", "061": "Manhattan",
                "081": "Queens", "085": "Staten Island"}

HMDA_URL = (
    "https://files.consumerfinance.gov/hmda-historic-loan-data/"
    "hmda_2017_ny_all-records_labels.zip"
)

def download_hmda():
    path = os.path.join(OUTDIR, "hmda_nyc.csv")
    print("Downloading HMDA NY 2017 data from CFPB...")
    try:
        req = urllib.request.Request(HMDA_URL, headers={"User-Agent": "CityPulse/1.0"})
        with urllib.request.urlopen(req, timeout=60) as resp:
            zip_bytes = resp.read()

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            csv_name = next(n for n in zf.namelist() if n.endswith(".csv"))
            with zf.open(csv_name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"))
                rows = []
                for row in reader:
                    county = str(row.get("county_code", "")).zfill(3)
                    if county not in NYC_COUNTIES:
                        continue
                    rows.append({
                        "loan_id": row.get("sequence_number", ""),
                        "county_code": county,
                        "borough": NYC_COUNTIES[county],
                        "census_tract": row.get("census_tract_number", ""),
                        "action_taken": row.get("action_taken", ""),
                        "action_taken_name": row.get("action_taken_name", ""),
                        "applicant_race": row.get("applicant_race_name_1", ""),
                        "loan_amount": int(float(row.get("loan_amount_000s") or 0)) * 1000,
                        "income": row.get("applicant_income_000s", ""),
                        "loan_purpose": row.get("loan_purpose_name", ""),
                        "loan_type": row.get("loan_type_name", ""),
                        "denial_reason_1": row.get("denial_reason_name_1", ""),
                        "year": row.get("as_of_year", "2017"),
                    })

        if not rows:
            raise ValueError("No NYC rows found in HMDA data")

        _write_csv(path, rows)
        print(f"  → {len(rows)} NYC rows written to hmda_nyc.csv (real CFPB data)")
    except Exception as e:
        print(f"  Download failed ({e}), falling back to synthetic HMDA data...")
        _synthetic_hmda(path)


def _synthetic_hmda(path):
    """Fallback synthetic HMDA data based on real NYC denial rate patterns."""
    boroughs = list(NYC_COUNTIES.items())
    races = ["White", "Black or African American", "Hispanic or Latino",
             "Asian", "American Indian or Alaska Native", "Not Provided"]
    loan_purposes = ["Home Purchase", "Refinancing", "Home Improvement"]
    loan_types = ["Conventional", "FHA-insured", "VA-guaranteed"]
    denial_rates = {
        "White": 0.10, "Black or African American": 0.27,
        "Hispanic or Latino": 0.22, "Asian": 0.12,
        "American Indian or Alaska Native": 0.20, "Not Provided": 0.15,
    }
    denial_reasons = [
        "Debt-to-income ratio", "Credit history", "Collateral",
        "Insufficient cash", "Unverifiable information", "Employment history",
    ]
    rows = []
    for i in range(600):
        county_code, borough = random.choice(boroughs)
        race = random.choice(races)
        dr = denial_rates[race]
        r = random.random()
        if r < dr:
            action_taken, action_name, denial = 3, "Application Denied", random.choice(denial_reasons)
        elif r < dr + 0.05:
            action_taken, action_name, denial = 4, "Application Withdrawn", ""
        else:
            action_taken, action_name, denial = 1, "Loan Originated", ""
        income = random.randint(30, 250)
        rows.append({
            "loan_id": f"HMDA-{i+1:06d}",
            "county_code": county_code,
            "borough": borough,
            "census_tract": f"{county_code}{random.randint(100, 999):04d}.00",
            "action_taken": action_taken,
            "action_taken_name": action_name,
            "applicant_race": race,
            "loan_amount": income * random.randint(2500, 5000),
            "income": income,
            "loan_purpose": random.choice(loan_purposes),
            "loan_type": random.choice(loan_types),
            "denial_reason_1": denial,
            "year": random.randint(2019, 2022),
        })
    _write_csv(path, rows)
    print(f"  → {len(rows)} synthetic rows written to hmda_nyc.csv")


def _write_csv(path, rows):
    cols = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# 3. Delivery Workers — real DCWP quarterly aggregate data
#    Source: NYC DCWP Restaurant Delivery App Data (quarterly XLSX)
#    URL: https://www.nyc.gov/assets/dca/downloads/xlsx/Restaurant-Delivery-App-Data-Quarterly.xlsx
# ---------------------------------------------------------------------------
DCWP_URL = "https://www.nyc.gov/assets/dca/downloads/xlsx/Restaurant-Delivery-App-Data-Quarterly.xlsx"

# Minimum pay rule timeline
# Jan 2023: $17.96/hr minimum enacted
# Jul 2023: raised to $19.56/hr
# Apr 2024: raised to $21.44/hr
# Apr 2025: raised to $21.44/hr (maintained)


def download_delivery_workers():
    path = os.path.join(OUTDIR, "delivery_workers.csv")
    print("Downloading NYC DCWP delivery worker quarterly data...")
    try:
        req = urllib.request.Request(DCWP_URL, headers={"User-Agent": "CityPulse/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()

        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb["Workers"]
        all_rows = list(ws.iter_rows(values_only=True))

        # Row 0: headers — quarter labels starting at index 1
        quarters = [str(c) for c in all_rows[0][1:] if c is not None]

        # Index metrics by stripped row label
        metric_map = {}
        for row in all_rows[1:]:
            label = str(row[0]).strip() if row[0] else None
            if label:
                metric_map[label] = [row[i + 1] for i in range(len(quarters))]

        def get(label):
            return metric_map.get(label, [None] * len(quarters))

        total_workers   = get("Total workers")
        avg_hours_qtr   = get("Average hours")          # avg hours per worker per quarter
        earnings_hr     = get("Earnings per hour")
        pay_hr          = get("Pay per hour")
        tips_hr         = get("Tips per hour")
        deliveries_hr   = get("Deliveries per hour")
        avg_earnings    = get("Average earnings")       # avg total earnings per worker per quarter

        rows = []
        for i, quarter in enumerate(quarters):
            # Determine if minimum pay rule was in effect
            year = int(quarter.split()[1])
            q    = quarter.split()[0]          # "Q1", "Q2", etc.
            if year < 2023:
                min_pay_status = "Pre-minimum pay rule"
            elif year == 2023 and q == "Q1":
                min_pay_status = "Pre-minimum pay rule"
            elif year == 2023:
                min_pay_status = "Minimum pay $17.96/hr"
            elif year == 2024 and q in ("Q1", "Q2"):
                min_pay_status = "Minimum pay $19.56/hr"
            else:
                min_pay_status = "Minimum pay $21.44/hr"

            def fmt(v, decimals=2):
                try:
                    return round(float(v), decimals) if v is not None else None
                except (TypeError, ValueError):
                    return None

            rows.append({
                "quarter":            quarter,
                "year":               year,
                "total_workers":      fmt(total_workers[i], 0),
                "avg_hours_per_qtr":  fmt(avg_hours_qtr[i]),
                "earnings_per_hour":  fmt(earnings_hr[i]),
                "pay_per_hour":       fmt(pay_hr[i]),
                "tips_per_hour":      fmt(tips_hr[i]),
                "deliveries_per_hour":fmt(deliveries_hr[i]),
                "avg_earnings_per_qtr": fmt(avg_earnings[i]),
                "min_pay_status":     min_pay_status,
            })

        _write_csv(path, rows)
        print(f"  → {len(rows)} quarterly rows written to delivery_workers.csv (real DCWP data)")
    except Exception as e:
        print(f"  Download failed ({e}), falling back to synthetic data...")
        _synthetic_delivery_workers(path)


def _synthetic_delivery_workers(path):
    """Fallback: quarterly trend data calibrated to DCWP study figures."""
    quarters = [
        ("Q1 2022", 2022, 13.40, 7.08, 6.32, 1.59, "Pre-minimum pay rule"),
        ("Q2 2022", 2022, 12.09, 6.17, 5.91, 1.52, "Pre-minimum pay rule"),
        ("Q3 2022", 2022, 12.87, 6.57, 6.30, 1.65, "Pre-minimum pay rule"),
        ("Q4 2022", 2022, 13.21, 6.72, 6.49, 1.71, "Pre-minimum pay rule"),
        ("Q1 2023", 2023, 15.43, 9.12, 6.31, 1.78, "Minimum pay $17.96/hr"),
        ("Q2 2023", 2023, 16.02, 10.11, 5.91, 1.85, "Minimum pay $17.96/hr"),
        ("Q3 2023", 2023, 18.44, 13.22, 5.22, 2.01, "Minimum pay $19.56/hr"),
        ("Q4 2023", 2023, 19.11, 14.03, 5.08, 2.10, "Minimum pay $19.56/hr"),
        ("Q1 2024", 2024, 21.33, 17.44, 3.89, 2.31, "Minimum pay $21.44/hr"),
        ("Q2 2024", 2024, 22.15, 18.52, 3.63, 2.42, "Minimum pay $21.44/hr"),
    ]
    rows = [{
        "quarter": q, "year": yr, "avg_hours_per_qtr": None,
        "earnings_per_hour": eph, "pay_per_hour": pph, "tips_per_hour": tph,
        "deliveries_per_hour": dph, "avg_earnings_per_qtr": None,
        "min_pay_status": status,
    } for q, yr, eph, pph, tph, dph, status in quarters]
    _write_csv(path, rows)
    print(f"  → {len(rows)} synthetic quarterly rows written to delivery_workers.csv")


# ---------------------------------------------------------------------------
# 4. PPP NYC — real SBA PPP data ($150K+ loans), streamed and filtered to NYC
# ---------------------------------------------------------------------------
PPP_URL = (
    "https://data.sba.gov/dataset/8aa276e2-6cab-4f86-aca4-a7dde42adf24"
    "/resource/c1275a03-c25c-488a-bd95-403c4b2fa036"
    "/download/public_150k_plus_240930.csv"
)

# NYC zip code prefix → borough (first 3 digits of 5-digit zip)
NYC_ZIP_BOROUGH = {
    "100": "Manhattan", "101": "Manhattan", "102": "Manhattan",
    "103": "Staten Island",
    "104": "Bronx",
    "110": "Queens", "111": "Queens", "114": "Queens", "115": "Queens", "116": "Queens",
    "112": "Brooklyn", "113": "Brooklyn",
}


def download_ppp():
    path = os.path.join(OUTDIR, "ppp_nyc.csv")
    print("Downloading SBA PPP data (streaming, filtering to NYC)...")
    try:
        req = urllib.request.Request(PPP_URL, headers={"User-Agent": "CityPulse/1.0"})
        out_cols = [
            "loan_id", "business_name", "zip", "borough", "naics_code",
            "business_type", "loan_amount", "jobs_retained", "lender",
            "race_ethnicity", "forgiveness_amount", "fully_forgiven",
            "loan_status", "loan_year",
        ]
        rows_written = 0
        with urllib.request.urlopen(req, timeout=120) as resp, \
             open(path, "w", newline="", encoding="utf-8") as out_f:

            writer = csv.DictWriter(out_f, fieldnames=out_cols)
            writer.writeheader()

            text_stream = io.TextIOWrapper(resp, encoding="utf-8", errors="replace")
            reader = csv.DictReader(text_stream)
            for row in reader:
                if row.get("ProjectState", "").strip().upper() != "NY":
                    continue
                zip5 = str(row.get("ProjectZip", "")).strip()[:5].zfill(5)
                borough = NYC_ZIP_BOROUGH.get(zip5[:3])
                if not borough:
                    continue
                try:
                    loan_amt = float(row.get("CurrentApprovalAmount") or row.get("InitialApprovalAmount") or 0)
                except ValueError:
                    loan_amt = 0
                try:
                    forgiven = float(row.get("ForgivenessAmount") or 0)
                except ValueError:
                    forgiven = 0
                date = row.get("DateApproved", "")
                year = date[:4] if date else "2020"
                writer.writerow({
                    "loan_id": row.get("LoanNumber", ""),
                    "business_name": row.get("BorrowerName", ""),
                    "zip": zip5,
                    "borough": borough,
                    "naics_code": row.get("NAICSCode", ""),
                    "business_type": row.get("BusinessType", ""),
                    "loan_amount": round(loan_amt, 2),
                    "jobs_retained": row.get("JobsReported", ""),
                    "lender": row.get("OriginatingLender", ""),
                    "race_ethnicity": row.get("Race", ""),
                    "forgiveness_amount": round(forgiven, 2),
                    "fully_forgiven": "Yes" if forgiven >= loan_amt * 0.9 and forgiven > 0 else "No",
                    "loan_status": row.get("LoanStatus", ""),
                    "loan_year": year,
                })
                rows_written += 1

        if rows_written == 0:
            raise ValueError("No NYC PPP rows found")
        print(f"  → {rows_written} NYC PPP rows written to ppp_nyc.csv (real SBA data)")
    except Exception as e:
        print(f"  Download failed ({e}), falling back to synthetic PPP data...")
        _synthetic_ppp(path)


def _synthetic_ppp(path):
    naics_map = {
        "722511": "Full-Service Restaurants", "812112": "Beauty Salons",
        "541110": "Offices of Lawyers", "621111": "Offices of Physicians",
        "722513": "Limited-Service Restaurants",
    }
    borough_zips = {
        "Manhattan": "10001", "Brooklyn": "11201",
        "Bronx": "10451", "Queens": "11355", "Staten Island": "10301",
    }
    rows = []
    for i in range(400):
        naics = random.choice(list(naics_map.keys()))
        borough = random.choice(list(borough_zips.keys()))
        loan_amount = random.randint(150000, 500000)
        forgiven = round(loan_amount * random.uniform(0.9, 1.0), 2) if random.random() < 0.85 else 0
        rows.append({
            "loan_id": f"PPP-{i+1:07d}", "business_name": f"Business {i+1}",
            "zip": borough_zips[borough], "borough": borough,
            "naics_code": naics, "business_type": naics_map[naics],
            "loan_amount": loan_amount, "jobs_retained": random.randint(1, 20),
            "lender": "Unknown", "race_ethnicity": "Unanswered",
            "forgiveness_amount": forgiven,
            "fully_forgiven": "Yes" if forgiven > 0 else "No",
            "loan_status": "Paid in Full",
            "loan_year": random.choice(["2020", "2021"]),
        })
    _write_csv(path, rows)
    print(f"  → {len(rows)} synthetic rows written to ppp_nyc.csv")


if __name__ == "__main__":
    download_algorithmic_tools()
    download_hmda()
    download_delivery_workers()
    download_ppp()
    print("\nAll datasets ready in mcp_server/data/")
