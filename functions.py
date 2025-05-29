import re
import pandas as pd
from datetime import timedelta
from dateutil import parser
import numpy as np

def _check_and_rename_cols(df, required_cols, col_map):
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Thiếu cột: {col}")
    return df.rename(columns=col_map)

def validate_and_format_for_1Aperiods(df, required_cols, col_map, export_cols):
    df2 = _check_and_rename_cols(df, required_cols, col_map)
    df2 = df2[df2['Route'].astype(str).str.strip().str.upper() != 'NON OPERATING']

    records = []
    for idx, row in df2.iterrows():
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
    df_out = pd.DataFrame(records)
    return df_out[export_cols] if export_cols else df_out

def validate_and_format_for_1A_market_report(df, required_cols, col_map, export_cols):
    day_map = {
        "MON": "1", "TUE": "2", "WED": "3", "THU": "4",
        "FRI": "5", "SAT": "6", "SUN": "7"
    }
    df2 = _check_and_rename_cols(df, required_cols, col_map)

    records = []
    for idx, row in df2.iterrows():
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
    df_out = pd.DataFrame(records)
    return df_out[export_cols] if export_cols else df_out

def validate_and_format_for_AIMS(df, required_cols, col_map, export_cols, aircraft_table):
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Thiếu cột: {col}")

    def is_footer_row(row):
        s = str(row["DATE"]).strip().upper()
        return (
            s.startswith("TOTAL RECORD") or
            s.startswith('"GENERATED ON') or
            s.startswith("GENERATED ON")
        )
    df = df[~df.apply(is_footer_row, axis=1)].reset_index(drop=True)

    df["OperationDate"] = df["DATE"].apply(
        lambda d: pd.to_datetime(str(d), dayfirst=True, errors="coerce").date()
        if pd.notna(d) and str(d).strip() != "" else np.nan
    )
    if df["OperationDate"].isna():
        idxs = df.index[df["OperationDate"].isna()]
        raise Exception(f"Lỗi chuyển đổi ngày tại các dòng: {', '.join(str(i+2) for i in idxs)}")

    col_map_ = {k: v for k, v in col_map.items() if k != "DATE"}
    df2 = df.rename(columns=col_map_).copy()

    c_values, y_values = [], []
    for val in df2["AC CONFIG"]:
        if pd.isna(val) or str(val).strip() == "":
            c, y = 0, 0
        else:
            parts = [int(x) for x in re.findall(r"\d+", str(val))]
            c = parts[1] if len(parts) > 1 else 0
            y = parts[2] if len(parts) > 2 else 0
        c_values.append(c)
        y_values.append(y)
    df2["C"] = c_values
    df2["Y"] = y_values

    aircraft_df = pd.DataFrame(aircraft_table["rows"], columns=aircraft_table["columns"])
    aircraft_df["C"] = aircraft_df["C"].astype(int)
    aircraft_df["Y"] = aircraft_df["Y"].astype(int)
    df2["ACV"] = ""
    df2["SaleableCfg"] = ""

    for idx, row in df2.iterrows():
        reg = str(row["RegNr."]).strip().upper()
        c = int(row["C"])
        y = int(row["Y"])
        matched = aircraft_df[
            (aircraft_df["RegNr."].astype(str).str.upper() == reg) &
            (aircraft_df["C"] == c) &
            (aircraft_df["Y"] == y)
        ]
        if not matched.empty:
            df2.at[idx, "ACV"] = matched.iloc[0]["ACV"]
            df2.at[idx, "SaleableCfg"] = matched.iloc[0]["SaleableCfg"]

    for col in export_cols:
        if col not in df2.columns:
            df2[col] = None

    return df2[export_cols].copy()