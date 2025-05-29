import re
import pandas as pd
from datetime import timedelta
from dateutil import parser


def validate_and_format_for_1Aperiods(df, required_cols):
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Thiếu cột: {col}")

    df = df[df['Route'].astype(str).str.strip().str.upper() != 'NON OPERATING']
    records = []

    for idx, row in df.iterrows():
        row = row.copy()
        flight_raw = str(row['Flight']).strip().upper()
        match = re.match(r"^(\S+)[\s\-]+(\S+)$", flight_raw)
        if not match:
            raise Exception(
                f"Dòng {idx+2}: Sai định dạng Flight: '{row['Flight']}'. "
                f"Phải có dạng 'OL FlightNbr', ví dụ 'QH 73' hoặc 'QH-73'."
            )
        ol, flight_nbr = match.groups()
        start_date = parser.parse(str(row['Start']).strip(), dayfirst=True, fuzzy=True)
        end_val = row['End']
        end_date = start_date if pd.isna(end_val) or str(end_val).strip() in ["", "nan"] else pd.to_datetime(str(end_val).strip(), format="%d%b%y", errors="coerce")
        if pd.isna(end_date):
            raise Exception(f"Dòng {idx+2}: Sai định dạng End: {row['End']}")

        freq_digits = ''.join([c for c in str(row['Frequency']).strip() if c in '1234567'])
        if not freq_digits:
            raise Exception(f"Dòng {idx+2}: Sai định dạng Frequency: {row['Frequency']}")
        freq_days = set(int(c) for c in freq_digits)

        route_str = str(row['Route']).strip().upper()
        if len(route_str) < 7 or '-' not in route_str:
            raise Exception(f"Dòng {idx+2}: Sai định dạng Route: '{row['Route']}' (phải có dạng AAA-BBB)")
        dep, arr = route_str[:3], route_str[-3:]

        config_code_str = str(row['Config Code']).strip()
        if len(config_code_str) < 3:
            raise Exception(f"Dòng {idx+2}: Sai định dạng Config Code: '{row['Config Code']}'")
        acv, saleable_cfg = config_code_str[:3], config_code_str[-3:]

        cur_date = start_date
        while cur_date <= end_date:
            if cur_date.isoweekday() in freq_days:
                new_row = {
                    'Type': row['Type'],
                    'OL': ol,
                    'FlightNbr': flight_nbr,
                    'OperationDate': cur_date.date(),
                    'Frequency': freq_digits,
                    'I/D': row['I/D'],
                    'DEP': dep,
                    'ARR': arr,
                    'Svc': row['Svc'],
                    'ACV': acv,
                    'SaleableCfg': saleable_cfg,
                    'Codeshare': row['Codeshare']
                }
                records.append(new_row)
            cur_date += timedelta(days=1)
    return pd.DataFrame(records)

def validate_and_format_for_1A_market_report(df):
    day_map = {
        "MON": "1", "TUE": "2", "WED": "3", "THU": "4",
        "FRI": "5", "SAT": "6", "SUN": "7"
    }
    required_cols = ["Flt Dt", "Al", "Flt", "Day", "Dep", "Brd", "Off", "CAP(C)", "CAP(Y)", "Eqp"]
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Thiếu cột: {col}")

    records = []
    for idx, row in df.iterrows():
        try:
            operation_date = parser.parse(str(row["Flt Dt"]).strip(), dayfirst=True, fuzzy=True)
            ol = str(row["Al"]).strip().upper()
            flight_nbr = str(row["Flt"]).strip().upper()
            freq = day_map.get(str(row["Day"]).strip().upper())
            if not freq:
                raise Exception(f"Dòng {idx+2}: Sai định dạng Day: {row['Day']}")

            std = str(row["Dep"]).strip().upper()
            dep = str(row["Brd"]).strip().upper()
            arr = str(row["Off"]).strip().upper()
            c, y = row["CAP(C)"], row["CAP(Y)"]
            actype = str(row["Eqp"]).strip().upper()

            new_row = {
                "OperationDate": operation_date.date(),
                "OL": ol,
                "FlightNbr": flight_nbr,
                "Frequency": freq,
                "STD": std,
                "DEP": dep,
                "ARR": arr,
                "C": c,
                "Y": y,
                "ACtype": actype
            }
            records.append(new_row)
        except Exception as e:
            raise Exception(f"Lỗi dòng {idx+2}: {e}")
    return pd.DataFrame(records)