import re
import pandas as pd
import numpy as np
import libs
import config
from datetime import timedelta
from dateutil import parser
from libs import parse_1A_date


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
        start_date = parse_1A_date(row['Start'])
        end_val = row['End']
        end_date = start_date if pd.isna(end_val) or str(end_val).strip() in ["", "nan"] else parse_1A_date(end_val)
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
                    'OperationDate': cur_date.strftime('%d-%b-%y'),
                    'Frequency': str(cur_date.isoweekday()),
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
    df2 = _check_and_rename_cols(df, required_cols, col_map)
    records = []
    for idx, row in df2.iterrows():
        try:
            operation_date = parse_1A_date(row["Flt Dt"])
            ol = str(row["Al"]).strip().upper()
            flight_nbr = str(row["Flt"]).strip().upper()
            freq = str(operation_date.isoweekday())
            std = str(row["Dep"]).strip()
            dep = str(row["Brd"]).strip()
            arr = str(row["Off"]).strip()
            c, y = row["CAP(C)"], row["CAP(Y)"]
            actype = str(row["Eqp"]).strip()
            new_row = {
                "OperationDate": operation_date.strftime('%d-%b-%y'),  # string for DB
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
            print(f"Lỗi dòng {idx+2}: {e}")
            raise
    df_out = pd.DataFrame(records)
    if export_cols:
        return df_out[export_cols]
    return df_out

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

    def try_parse_date(val):
        val = str(val).strip()
        for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%d%b%y", "%d-%b-%y", "%Y-%m-%d"):
            try:
                return pd.to_datetime(val, format=fmt, errors="raise")
            except Exception:
                continue
        return pd.to_datetime(val, dayfirst=True, errors="coerce")  # fallback

    df = df[~df.apply(is_footer_row, axis=1)].reset_index(drop=True)

    # Chuyển đổi cột DATE sang định dạng chuẩn
    df["OperationDate"] = df["DATE"].apply(
        lambda d: try_parse_date(d).strftime('%d-%b-%y') if pd.notna(d) and str(d).strip() != "" else np.nan
    )
    if df["OperationDate"].isna().any():
        idxs = df.index[df["OperationDate"].isna()]
        raise Exception(f"Lỗi chuyển đổi ngày tại các dòng: {', '.join(str(i+2) for i in idxs)}")

    # Đổi tên cột (trừ DATE đã xử lý riêng)
    col_map_ = {k: v for k, v in col_map.items() if k != "DATE"}
    df2 = df.rename(columns=col_map_).copy()

    # Tách cấu hình AC CONFIG thành C/Y
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

    # Ánh xạ thông tin từ bảng máy bay
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

    df2["Frequency"] = df2["OperationDate"].apply(
        lambda d: str(pd.to_datetime(d, format='%d-%b-%y', errors='coerce').isoweekday()) if pd.notna(d) else None
    )

    for col in export_cols:
        if col not in df2.columns:
            df2[col] = None

    return df2[export_cols].copy()


def validate_and_format_for_SKD(df, required_cols, col_map, export_cols, aircraft_table):
    import pandas as pd

    # Kiểm tra cột bắt buộc
    for col in required_cols:
        if col not in df.columns:
            raise Exception(f"Thiếu cột bắt buộc: {col}")

    df2 = df.rename(columns=col_map).copy()

    # Tách FLT NBR thành OL và FlightNbr
    flt_col = "FLT NBR" if "FLT NBR" in df.columns else None
    if flt_col:
        df2["OL"] = df[flt_col].astype(str).str[:2]
        df2["FlightNbr"] = df[flt_col].astype(str).str[2:]
    else:
        df2["OL"] = ""
        df2["FlightNbr"] = ""

    expanded_rows = []
    aircraft_df = pd.DataFrame(aircraft_table["rows"], columns=aircraft_table["columns"])

    for idx, row in df2.iterrows():
        from_raw = row.get("From", "")
        to_raw = row.get("To", "")
        if pd.isna(from_raw) or str(from_raw).strip() == "":
            continue
        if pd.isna(to_raw) or str(to_raw).strip() == "":
            continue

        try:
            from_date = pd.to_datetime(from_raw, errors="coerce", dayfirst=True)
            to_date = pd.to_datetime(to_raw, errors="coerce", dayfirst=True)
            if pd.isna(from_date) or pd.isna(to_date):
                continue
        except Exception:
            continue

        freq_digits = ''.join([c for c in str(row.get('Frequency', '')).strip() if c in '1234567'])
        if not freq_digits:
            continue
        freq_days = set(int(c) for c in freq_digits)

        cur_date = from_date
        while cur_date <= to_date:
            if cur_date.isoweekday() in freq_days:
                new_row = row.copy()
                new_row["OperationDate"] = cur_date.strftime('%d-%b-%y')
                new_row["Frequency"] = str(cur_date.isoweekday())

                # Bổ sung OL và FlightNbr từ row đã tách
                new_row["OL"] = row.get("OL", "")
                new_row["FlightNbr"] = row.get("FlightNbr", "")

                # Tách cấu hình ghế
                cfg_parts = [int(x) for x in str(new_row.get("New CFG", "")).split("/") if x.isdigit()]
                new_row["C(S)"] = cfg_parts[0] if len(cfg_parts) > 0 else 0
                new_row["Y(S)"] = cfg_parts[2] if len(cfg_parts) > 2 else 0

                # Ghép thông tin từ bảng aircraft
                reg = str(new_row.get("TAIL #", "")).strip().upper()
                match = aircraft_df[aircraft_df["RegNr."].astype(str).str.upper() == reg]
                if not match.empty:
                    matched_row = match.iloc[0]
                    new_row["RegNr."] = matched_row.get("RegNr.", "")
                    new_row["ACV"] = matched_row.get("ACV", "")
                    new_row["SaleableCfg"] = matched_row.get("SaleableCfg", "")
                    new_row["C"] = matched_row.get("C", "")
                    new_row["Y"] = matched_row.get("Y", "")
                    new_row["ACtype"] = matched_row.get("ACtype", "")
                else:
                    new_row["RegNr."] = ""
                    new_row["ACV"] = ""
                    new_row["SaleableCfg"] = ""
                    new_row["C"] = ""
                    new_row["Y"] = ""
                    new_row["ACtype"] = ""

                expanded_rows.append(new_row)
            cur_date += pd.Timedelta(days=1)

    df_expanded = pd.DataFrame(expanded_rows)

    if df_expanded.empty:
        raise Exception("Không có dòng dữ liệu hợp lệ sau khi tách From/To và DOW")

    # Đảm bảo các cột theo export_cols đều tồn tại
    final_cols = export_cols
    for col in final_cols:
        if col not in df_expanded.columns:
            df_expanded[col] = ""

    # Làm sạch RegNr.
    if "RegNr." in df_expanded.columns:
        df_expanded["RegNr."] = df_expanded["RegNr."].fillna("")

    return df_expanded[final_cols].reset_index(drop=True)


def process_merge_1a(db_engine):
    """
    Đọc bảng Periods_1A và Market_Report_1A, merge, lưu vào DB và trả về các DataFrame:
    - merged (Merged_1A)
    - not_in_market (NOT_in_Market_Report)
    - not_in_periods (NOT_in_Periods)
    """
    merged, not_in_market, not_in_periods = libs.create_merged_1a(db_engine)
    # Lưu vào DB
    merged.to_sql("Merged_1A", db_engine, if_exists="replace", index=False)
    not_in_market.to_sql("NOT_in_Market_Report", db_engine, if_exists="replace", index=False)
    not_in_periods.to_sql("NOT_in_Periods", db_engine, if_exists="replace", index=False)
    return merged, not_in_market, not_in_periods

def process_compare_aims_1a(db_engine):
    """
    Tạo Merged_1A và các bảng liên quan, trả về dict các DataFrame để xuất Excel
    """
    merged, not_in_market, not_in_periods = process_merge_1a(db_engine)
    return {
        "Merged_1A": merged,
        "NOT_in_Market_Report": not_in_market,
        "NOT_in_Periods": not_in_periods
    }

def process_compare_skd_1a(db_engine):
    """
    Tạo Merged_1A nếu cần, so sánh với SKD_Data, trả về dict các DataFrame để xuất Excel
    """
    # Đảm bảo đã có Merged_1A (gọi lại merge nếu cần)
    merged, _, _ = process_merge_1a(db_engine)

    # Đọc SKD_Data và Merged_1A từ DB (đồng nhất kiểu cột)
    skd_data = pd.read_sql("SKD_Data", db_engine)
    merged_1a = pd.read_sql("Merged_1A", db_engine)

    compare_result, still_in_market_report = libs.compare_skd_and_1a(skd_data, merged_1a)

    # Lưu vào DB
    compare_result.to_sql("Compare_SKD_1A_Data", db_engine, if_exists="replace", index=False)
    if still_in_market_report is not None and not still_in_market_report.empty:
        still_in_market_report.to_sql("STILL_in_Market_Report", db_engine, if_exists="replace", index=False)
    return {
        "Compare_SKD_1A_Data": compare_result,
        "STILL_in_Market_Report": still_in_market_report
    }