#%%
import random
import pandas as pd

# Read the CSV file
df = pd.read_csv("award_status_20250512.csv")

# Melt the DataFrame to create a long format
melted_df = df.melt(id_vars=["Unnamed: 0"], var_name="Scout", value_name="Status_Date")

# Rename the first column to "Award"
melted_df.rename(columns={"Unnamed: 0": "Award"}, inplace=True)

# Drop rows where Status_Date is NaN
melted_df.dropna(subset=["Status_Date"], inplace=True)

# Parse Status and Date from the Status_Date column
melted_df["Status"] = melted_df["Status_Date"].str.extract(r"^(Approved|Awarded|In Progress|Not Started)")
melted_df["Date"] = melted_df["Status_Date"].str.extract(r"(\d{1,2}/\d{1,2}/\d{2,4})")
melted_df["Date"] = pd.to_datetime(melted_df["Date"], format="%m/%d/%y")

# Drop the original Status_Date column
melted_df.drop(columns=["Status_Date"], inplace=True)

awards = melted_df[(melted_df['Status'] == "Awarded") | (melted_df['Status'] == "Approved")][melted_df['Date'] > pd.Timestamp("2025-03-01")].sort_values("Award")
awards.to_csv("Awards.csv", index=False)
#%%
# Create and write to a printable document a readable list showing Award: then a randomized list of the scouts who earned it
# Format as 3 columns. Prevent the DIVs from breaking across pages

with open("Awards.html", "w") as f:
    f.write("<html>\n<head>\n<title>Awards</title>\n</head>\n<body>\n")
    # Format as 3 columns of text
    f.write("<div style='display: flex; flex-wrap: wrap;'>\n")
    for award in awards['Award'].unique():
        # where the status is Approved, add (no loop) to the end of the Scout name (so, scoutname (no loop)<|cursor|>). Make a new column called scout_status
        awards.loc[awards['Award'] == award, 'scout_status'] = awards.apply(lambda row: f"{row['Scout']} (no loop)" if row['Status'] == "Approved" else row['Scout'], axis=1)
        scouts = awards[awards['Award'] == award]['scout_status'].to_list()
        random.shuffle(scouts)
        f.write(f"<div style='flex: 1; min-width: 300px; page-break-inside: avoid;'>\n<h2>{award}</h2><h3>Scouts:</h3><ul>{"\n".join(f"<li>{scout}</li>" for scout in scouts)}</ul></div>")
    f.write("</div>\n")
    f.write("</body>\n</html>")
# %%
