#%%
from pathlib import Path

import pandas as pd

INPUT_PATH = Path("progress_reports/2026-01/RosterReport_Pack0500_Scouts_parents_20260103.csv")
OUTPUT_PATH = Path("roster_20260103_fixed.csv")

df = pd.read_csv(INPUT_PATH, header=1, encoding="latin-1")

# Normalize column names so we can reason about the offsets consistently.
df = df.rename(
  columns={
    " ": "ID",
    "Unnamed: 7": "Address 2",
    "Unnamed: 12": "DropMe",
  }
)

# Some rows (secondary guardian entries) start with blank ID/Name values and
# have all meaningful data shifted into the columns starting at "Den".
mask_missing_id = df["ID"].isna()
shift_source = df.copy(deep=True)

df.loc[mask_missing_id, "Parent/Guardian Name "] = shift_source.loc[mask_missing_id, "Den"]
df.loc[mask_missing_id, "Relationship"] = shift_source.loc[mask_missing_id, "Parent/Guardian Name "]
df.loc[mask_missing_id, "Address"] = shift_source.loc[mask_missing_id, "Relationship"]
df.loc[mask_missing_id, "Den"] = None

# Contact data for the shifted rows lands in later columns; move it back into
# the standard phone/email fields.
df.loc[mask_missing_id, "Home Phone"] = shift_source.loc[mask_missing_id, "Address"]
df.loc[mask_missing_id, "Work Phone"] = shift_source.loc[mask_missing_id, "Home Phone"]
df.loc[mask_missing_id, "Mobile Phone"] = shift_source.loc[mask_missing_id, "Work Phone"]

email_fallback = (
  shift_source.loc[mask_missing_id, ["Email", "Mobile Phone", "Address 2"]]
  .bfill(axis=1)
  .iloc[:, 0]
)
df.loc[mask_missing_id, "Email"] = email_fallback
df.loc[mask_missing_id, "Address 2"] = None

# Drop unusable columns from the export.
df = df.drop(columns=["DropMe"])

# Fill forward the scout metadata so each guardian row stays associated with
# the correct youth member.
df["First Name"] = df["First Name"].ffill()
df["Last Name"] = df["Last Name"].ffill()
df["Den"] = df["Den"].ffill()
df["ID"] = df["ID"].ffill()

df.to_csv(OUTPUT_PATH, index=False)
#%%
