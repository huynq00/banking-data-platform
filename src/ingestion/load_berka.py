"""Load the Berka dataset into the Core Banking (OLTP) database.

Pipeline:  EXTRACT (read CSV)  →  TRANSFORM (clean + map to our schema)  →  LOAD (COPY)

Idempotent: every run wipes the tables and reloads them inside ONE transaction,
so running it twice gives exactly the same result, and a failed run changes nothing.

Usage (from the project root, with .venv activated and Postgres running):
    python -m src.ingestion.load_berka
"""

import io
import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import psycopg

from src.common.config import PROJECT_ROOT, oltp_config
from src.ingestion.vn_pii import VietnamesePII

log = logging.getLogger("load_berka")

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "berka"

# ---------------------------------------------------------------------------
# Assumptions (documented in docs/01-design.md)
# ---------------------------------------------------------------------------
SEED = 42                               # same seed → same fake names every run
YEAR_SHIFT = 28                         # the calendar repeats every 28 years: same weekdays,
                                        # same leap years → month-ends stay month-ends
CUTOFF_DATE = date(2026, 9, 28)         # shifted data after this date is in the future → dropped
CZK_TO_VND = 1000                       # simplification: 1 CZK ≈ 1,000 VND
TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")
BALANCE_TOLERANCE = 0.2                 # Berka rounds interest to 0.1 CZK

# ---------------------------------------------------------------------------
# Czech → our schema mappings (found during profiling)
# ---------------------------------------------------------------------------
TXN_TYPE_MAP = {"PRIJEM": "CREDIT", "VYDAJ": "DEBIT", "VYBER": "DEBIT"}

CHANNEL_MAP = {
    "VKLAD": "BRANCH",            # cash deposit
    "VYBER": "ATM",               # cash withdrawal
    "VYBER KARTOU": "CARD",       # card withdrawal
    "PREVOD Z UCTU": "TRANSFER",  # incoming transfer
    "PREVOD NA UCET": "TRANSFER", # outgoing transfer
}
SYSTEM_CHANNEL = "SYSTEM"         # no operation = interest posted by the bank itself

DESCRIPTION_MAP = {
    "UROK": "Interest credited",
    "SLUZBY": "Statement fee",
    "SIPO": "Household payment",
    "DUCHOD": "Pension",
    "POJISTNE": "Insurance payment",
    "UVER": "Loan repayment",
    "SANKC. UROK": "Penalty interest",
}

LOAN_STATUS_MAP = {
    "A": "PAID_OFF",  # contract finished, no problems
    "B": "DEFAULT",   # contract finished, loan not paid
    "C": "ACTIVE",    # running, OK so far
    "D": "OVERDUE",   # running, client in debt
}

INTEREST_RATE_BY_TERM = {12: 8.5, 24: 9.0, 36: 9.5, 48: 10.0, 60: 10.5}  # synthetic


# ===========================================================================
# EXTRACT
# ===========================================================================
def read_raw(name: str) -> pd.DataFrame:
    """Read a Berka CSV as text, and normalise every kind of 'empty' to NULL."""
    df = pd.read_csv(RAW_DIR / f"{name}.csv", sep=";", dtype=str, keep_default_na=False)
    df = df.apply(lambda col: col.str.strip())
    return df.replace("", None)


# ===========================================================================
# TRANSFORM helpers
# ===========================================================================
def parse_berka_date(yymmdd: pd.Series) -> pd.Series:
    """'930101' → 1993-01-01, then shift into our time window.

    Berka years are always 19YY. We never let pandas guess the century (%y),
    because it would turn '11' into 2011.
    """
    parsed = pd.to_datetime("19" + yymmdd, format="%Y%m%d")
    return (parsed + pd.DateOffset(years=YEAR_SHIFT)).dt.date


def decode_birth_number(birth_number: pd.Series) -> pd.DataFrame:
    """Berka encodes birth date + gender as YYMMDD, with MM + 50 for women."""
    yy = birth_number.str[0:2]
    mm = birth_number.str[2:4].astype(int)
    dd = birth_number.str[4:6]

    is_female = mm > 50
    real_mm = np.where(is_female, mm - 50, mm)
    yymmdd = yy + pd.Series(real_mm, index=birth_number.index).map("{:02d}".format) + dd

    return pd.DataFrame(
        {
            "date_of_birth": parse_berka_date(yymmdd),
            "gender": np.where(is_female, "F", "M"),
        },
        index=birth_number.index,
    )


def to_vnd(amount: pd.Series) -> pd.Series:
    return (amount.astype(float) * CZK_TO_VND).round(2)


# ===========================================================================
# TRANSFORM: one function per target table
# ===========================================================================
def transform_branches(district: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "branch_id": district["A1"].astype(int),
            "branch_code": "BR" + district["A1"].str.zfill(3),
            "name": district["A2"],
            "region": district["A3"],
        }
    )


def transform_customers(
    client: pd.DataFrame, disp: pd.DataFrame, accounts: pd.DataFrame
) -> pd.DataFrame:
    df = client.copy()
    df["customer_id"] = df["client_id"].astype(int)
    df["branch_id"] = df["district_id"].astype(int)
    df = df.join(decode_birth_number(df["birth_number"]))

    # A customer "joins" the bank when the account they are linked to is opened
    disp_ = disp.assign(
        client_id=disp["client_id"].astype(int), account_id=disp["account_id"].astype(int)
    )
    opened = disp_.merge(accounts[["account_id", "opened_at"]], on="account_id")
    first_seen = opened.groupby("client_id")["opened_at"].min()
    df["created_at"] = df["customer_id"].map(first_seen)

    # Berka has no PII → generate realistic Vietnamese PII, deterministically
    pii = VietnamesePII(seed=SEED)
    df["full_name"] = [pii.full_name(g) for g in df["gender"]]
    df["phone"] = [pii.phone() for _ in range(len(df))]
    df["email"] = [pii.email(n, c) for n, c in zip(df["full_name"], df["customer_id"])]
    df["address"] = [pii.address() for _ in range(len(df))]
    df["national_id"] = [
        make_national_id(cid, bid, dob, g)
        for cid, bid, dob, g in zip(
            df["customer_id"], df["branch_id"], df["date_of_birth"], df["gender"]
        )
    ]
    df["segment"] = "STANDARD"
    df["updated_at"] = df["created_at"]

    return df[
        [
            "customer_id", "full_name", "national_id", "date_of_birth", "gender",
            "phone", "email", "address", "branch_id", "segment", "created_at", "updated_at",
        ]
    ]


def make_national_id(customer_id: int, branch_id: int, dob: date, gender: str) -> str:
    """Vietnamese CCCD layout: 3-digit province + gender/century digit + YY + 6 digits.

    The last 6 digits are the customer_id, which guarantees uniqueness.
    """
    province = f"{branch_id:03d}"
    century_gender = {(19, "M"): 0, (19, "F"): 1, (20, "M"): 2, (20, "F"): 3}[
        (dob.year // 100, gender)
    ]
    return f"{province}{century_gender}{dob.year % 100:02d}{customer_id:06d}"


def transform_accounts(account: pd.DataFrame, disp: pd.DataFrame) -> pd.DataFrame:
    owners = disp[disp["type"] == "OWNER"][["account_id", "client_id"]]
    df = account.merge(owners, on="account_id", how="left", validate="one_to_one")

    df["account_id"] = df["account_id"].astype(int)
    df["account_number"] = "1903" + df["account_id"].astype(str).str.zfill(8)
    df["customer_id"] = df["client_id"].astype(int)
    df["branch_id"] = df["district_id"].astype(int)
    df["account_type"] = "CURRENT"
    df["currency"] = "VND"
    df["status"] = "ACTIVE"
    df["opened_at"] = [
        datetime.combine(d, time(8, 0), tzinfo=TIMEZONE) for d in parse_berka_date(df["date"])
    ]
    return df[
        [
            "account_id", "account_number", "customer_id", "branch_id",
            "account_type", "currency", "status", "opened_at",
        ]
    ]


def order_within_day(trans: pd.DataFrame) -> pd.DataFrame:
    """Berka has only DATES, so the order of transactions within a day is unknown.

    We rebuild it: starting from yesterday's closing balance, repeatedly pick the
    transaction whose `previous balance + amount == balance_after`.
    Returns trans sorted in the rebuilt order, with a `seq` column (0, 1, 2 … per day).
    """
    df = trans.assign(
        _interest_first=np.where(trans["k_symbol"] == "UROK", 0, 1)
    ).sort_values(["account_id", "date", "_interest_first", "trans_id"]).reset_index(drop=True)

    acc = df["account_id"].to_numpy()
    day = df["date"].to_numpy()
    signed = df["signed_amount"].to_numpy()
    balance = df["balance"].to_numpy()

    order = np.empty(len(df), dtype=np.int64)
    unresolved = 0
    i, n = 0, len(df)
    current_account, running_balance = None, None

    while i < n:
        j = i
        while j < n and acc[j] == acc[i] and day[j] == day[i]:
            j += 1
        if acc[i] != current_account:
            current_account, running_balance = acc[i], None

        candidates, pos = list(range(i, j)), i
        while candidates:
            pick = None
            if running_balance is None:
                pick = candidates[0]
            else:
                for k in candidates:
                    if abs(running_balance + signed[k] - balance[k]) <= BALANCE_TOLERANCE:
                        pick = k
                        break
                if pick is None:
                    unresolved += 1
                    pick = candidates[0]
            candidates.remove(pick)
            order[pos] = pick
            pos += 1
            running_balance = balance[pick]
        i = j

    log.info(
        "Rebuilt intra-day order: %s of %s transactions could not be chained (%.3f%%)",
        f"{unresolved:,}", f"{n:,}", 100 * unresolved / n,
    )
    df = df.iloc[order].reset_index(drop=True)
    df["seq"] = df.groupby(["account_id", "date"]).cumcount()
    return df.drop(columns="_interest_first")


def transform_transactions(trans: pd.DataFrame) -> pd.DataFrame:
    df = trans.copy()
    df["trans_id"] = df["trans_id"].astype(int)
    df["account_id"] = df["account_id"].astype(int)
    df["amount"] = df["amount"].astype(float)
    df["balance"] = df["balance"].astype(float)

    # Data quality rule: zero-amount rows carry no money → drop, but say so
    zero = df["amount"] == 0
    log.warning("Dropping %s transactions with amount = 0", f"{zero.sum():,}")
    df = df[~zero]

    future = parse_berka_date(df["date"]) > CUTOFF_DATE
    log.warning("Dropping %s transactions dated after %s", f"{future.sum():,}", CUTOFF_DATE)
    df = df[~future]

    df["txn_type"] = df["type"].map(TXN_TYPE_MAP)
    unknown = df["txn_type"].isna()
    if unknown.any():
        raise ValueError(f"Unknown transaction types: {df.loc[unknown, 'type'].unique()}")

    df["signed_amount"] = np.where(df["txn_type"] == "CREDIT", df["amount"], -df["amount"])
    df = order_within_day(df)

    # Business hours: first transaction of the day at 08:00, then one per hour
    txn_dates = parse_berka_date(df["date"])
    df["txn_time"] = [
        datetime.combine(d, time(8, 0), tzinfo=TIMEZONE) + timedelta(hours=int(s), seconds=int(t) % 3600)
        for d, s, t in zip(txn_dates, df["seq"], df["trans_id"])
    ]

    df["channel"] = df["operation"].map(CHANNEL_MAP).where(df["operation"].notna(), SYSTEM_CHANNEL)
    unknown = df["channel"].isna()
    if unknown.any():
        raise ValueError(f"Unknown operations: {df.loc[unknown, 'operation'].unique()}")

    df["counterparty_account"] = (df["bank"] + "-" + df["account"]).where(df["account"].notna())
    df["description"] = df["k_symbol"].map(DESCRIPTION_MAP)

    return pd.DataFrame(
        {
            "transaction_id": df["trans_id"],
            "account_id": df["account_id"],
            "txn_type": df["txn_type"],
            "amount": to_vnd(df["amount"]),
            "balance_after": to_vnd(df["balance"]),
            "channel": df["channel"],
            "counterparty_account": df["counterparty_account"],
            "description": df["description"],
            "txn_time": df["txn_time"],
            "created_at": df["txn_time"],  # historical data: recorded when it happened
        }
    )


def latest_balances(transactions: pd.DataFrame) -> pd.Series:
    """Current balance of each account = balance_after of its latest transaction."""
    last = transactions.sort_values("txn_time").groupby("account_id").tail(1)
    return last.set_index("account_id")["balance_after"]


def transform_loans(loan: pd.DataFrame) -> pd.DataFrame:
    df = loan.copy()
    df["loan_id"] = df["loan_id"].astype(int)
    df["account_id"] = df["account_id"].astype(int)
    df["amount"] = df["amount"].astype(float)
    df["payments"] = df["payments"].astype(float)
    df["term_months"] = df["duration"].astype(int)
    df["start_date"] = parse_berka_date(df["date"])
    future = df["start_date"] > CUTOFF_DATE
    log.warning("Dropping %s loans starting after %s", f"{future.sum():,}", CUTOFF_DATE)
    df = df[~future].copy()
    df["status"] = df["status"].map(LOAN_STATUS_MAP)
    df["interest_rate"] = df["term_months"].map(INTEREST_RATE_BY_TERM)

    # Berka gives no outstanding amount → estimate from months elapsed (synthetic)
    months_elapsed = (
        (CUTOFF_DATE.year - pd.to_datetime(df["start_date"]).dt.year) * 12
        + (CUTOFF_DATE.month - pd.to_datetime(df["start_date"]).dt.month)
    ).clip(upper=df["term_months"])
    remaining = (df["amount"] - df["payments"] * months_elapsed).clip(lower=df["payments"])

    df["outstanding_amount"] = np.select(
        [df["status"] == "PAID_OFF", df["status"] == "DEFAULT"],
        [0.0, df["payments"] * 3],  # default: assume 3 unpaid instalments
        default=remaining,
    )
    df["days_past_due"] = np.select(
        [df["status"] == "OVERDUE", df["status"] == "DEFAULT"],
        [30 + df["loan_id"] % 60, 360 + df["loan_id"] % 180],
        default=0,
    )

    return pd.DataFrame(
        {
            "loan_id": df["loan_id"],
            "account_id": df["account_id"],
            "principal_amount": to_vnd(df["amount"]),
            "outstanding_amount": to_vnd(df["outstanding_amount"]),
            "interest_rate": df["interest_rate"],
            "term_months": df["term_months"],
            "start_date": df["start_date"],
            "days_past_due": df["days_past_due"],
            "status": df["status"],
        }
    )


# ===========================================================================
# LOAD
# ===========================================================================
def copy_dataframe(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    """Bulk-load with COPY: far faster than INSERT for large volumes."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    columns = ", ".join(df.columns)
    with cur.copy(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)") as copy:
        copy.write(buffer.getvalue())
    log.info("Loaded %-12s %s rows", table, f"{len(df):,}")


def reset_identity(cur: psycopg.Cursor, table: str, column: str) -> None:
    """We inserted explicit IDs, so move the ID generator past the max ID."""
    cur.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
        f"(SELECT MAX({column}) FROM {table}))"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    log.info("EXTRACT: reading CSV files from %s", RAW_DIR)
    raw = {name: read_raw(name) for name in ["district", "client", "account", "disp", "loan", "trans"]}

    log.info("TRANSFORM")
    branches = transform_branches(raw["district"])
    accounts = transform_accounts(raw["account"], raw["disp"])
    customers = transform_customers(raw["client"], raw["disp"], accounts)
    transactions = transform_transactions(raw["trans"])
    loans = transform_loans(raw["loan"])
    accounts["balance"] = accounts["account_id"].map(latest_balances(transactions)).fillna(0)
    accounts["updated_at"] = accounts["opened_at"]

    log.info("LOAD")
    with psycopg.connect(oltp_config().conninfo) as conn:  # one transaction for everything
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE transactions, loans, accounts, customers, branches RESTART IDENTITY CASCADE"
            )
            copy_dataframe(cur, "branches", branches)
            copy_dataframe(cur, "customers", customers)
            copy_dataframe(cur, "accounts", accounts)
            copy_dataframe(cur, "transactions", transactions)
            copy_dataframe(cur, "loans", loans)

            for table, column in [
                ("branches", "branch_id"), ("customers", "customer_id"),
                ("accounts", "account_id"), ("transactions", "transaction_id"),
                ("loans", "loan_id"),
            ]:
                reset_identity(cur, table, column)
    log.info("Done ✔")


if __name__ == "__main__":
    main()
