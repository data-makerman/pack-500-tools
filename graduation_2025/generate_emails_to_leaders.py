#%%
"""
Read the contents of 2025 Graduation Campout Form.csv, split the data into ranks, and prepare emails as rich text to the rank den leaders.
The emails will contain information on which scouts will be camping, which/how many nights,
how many scouts will attend on Saturday for the day's activities, and how many scouts will attend crossover and dinner.
"""
import pandas as pd
from textwrap import dedent

df = pd.read_csv("2025 Graduation Campout Form.csv")

columns_rename = [
    "timestamp",
    "email",
    "scouts",
    "ranks",
    "attending_any",
    "camp_friday",
    "camp_saturday",
    "people_camping",
    "attending_saturday",
    "crossover_count",
    "dinner_count",
    "food_allergies",
    "comments"
]
df.columns = columns_rename

# make indicator columns for each rank, if the string in the ranks column contains the rank
ranks = df["ranks"].str.split(";").explode().unique()
for rank in ranks:
    # create a new column for each rank
    df[rank] = df["ranks"].apply(lambda x: True if rank in x else False)

# convert attending_saturday to bool, except handling values other than "No" or "Yes" as "True"
df["attending_saturday"] = df["attending_saturday"].apply(lambda x: False if x == "No" else False if x == "no" else True)


readable_column_rename_dict = {
    "timestamp": "Timestamp",
    "email": "Email",
    "scouts": "Scouts",
    "ranks": "Ranks",
    "attending_any": "Attending Any Part",
    "camp_friday": "Number Camping Friday",
    "camp_saturday": "Number Camping Saturday",
    "people_camping": "People Camping",
    "attending_saturday": "Attending Saturday",
    "crossover_count": "Number Attending Crossover",
    "dinner_count": "Number Attending Dinner",
    "food_allergies": "Food Allergies",
    "comments": "Comments"
}

def build_html_message(rank, df):
    num_scouts = df[df[rank]].shape[0]
    # Food allergies contains some poor strings like "Na" and "none" which should mean NaN
    food_allergies = df[df[rank]]["food_allergies"].dropna().tolist()
    food_allergies = [allergy for allergy in food_allergies if allergy not in ["Na", "none"]]
    food_allergies = [f"<li>{allergy.strip()}</li>" for allergy in food_allergies]
    food_allergies = "\n".join(food_allergies)

    table = df[df[rank]]

    attending_saturday_scout_names = table[table["attending_saturday"]]["scouts"].tolist()
    attending_saturday_scout_names = [f"<li>{scout_name.strip()}</li>" for scout_name in attending_saturday_scout_names]
    attending_saturday_scout_names = "\n".join(attending_saturday_scout_names)

    camping_both_nights_scout_names = table[(table["camp_friday"] > 0) & (table["camp_saturday"] > 0)]["scouts"].tolist()
    camping_both_nights_scout_names = [f"<li>{scout_name.strip()}</li>" for scout_name in camping_both_nights_scout_names]
    camping_both_nights_scout_names = "\n".join(camping_both_nights_scout_names)

    camping_friday_scout_names = table[(table["camp_friday"] > 0) & (table['camp_saturday']<=0)]["scouts"].tolist()
    camping_friday_scout_names = [f"<li>{scout_name.strip()}</li>" for scout_name in camping_friday_scout_names]
    camping_friday_scout_names = "\n".join(camping_friday_scout_names)

    camping_saturday_scout_names = table[(table["camp_saturday"] > 0) & (table['camp_friday']<=0)]["scouts"].tolist()
    camping_saturday_scout_names = [f"<li>{scout_name.strip()}</li>" for scout_name in camping_saturday_scout_names]
    camping_saturday_scout_names = "\n".join(camping_saturday_scout_names)

    crossover_scout_names = table[table["crossover_count"] > 0]["scouts"].tolist()
    crossover_scout_names = [f"<li>{scout_name.strip()}</li>" for scout_name in crossover_scout_names]
    crossover_scout_names = "\n".join(crossover_scout_names)

    message = f"""
Dear {rank} Leaders,
<div>Here are our records for your rank for this weekend's graduation ceremony and campout. We're looking forward to spending the weekend with you!</div>
<br />
<div>Please be prepared for:<ol>
<li>Saturday's lunch (Dens are responsible for Saturday's lunch!)</li>
<li>Any den activities you have planned</li>
<li>A den skit for the campfire on Saturday night</li>
</ol></div>
<br />
<div>The details below have been processed automatically from responses to the sign-up form.</div>

<h4>List of Scouts attending Saturday's activities and any listed siblings:</h4>
<ul>{attending_saturday_scout_names}</ul>

<h4>Food allergies:</h4>
<ul>{food_allergies}</ul>

<h4>List of Scouts camping both nights:</h4>
<ul>{camping_both_nights_scout_names}</ul>

<h4>List of Scouts camping Friday night only:</h4>
<ul>{camping_friday_scout_names}</ul>

<h4>List of Scouts camping Saturday night only:</h4>
<ul>{camping_saturday_scout_names}</ul>

<h4>List of Scouts attending crossover:</h4>
<ul>{crossover_scout_names}</ul>

<h4>Full details:</h4>
{table.drop(columns=["timestamp"]+list(ranks)).rename(columns=readable_column_rename_dict).fillna("").to_html(index=False, justify="left", border=1, classes="table table-striped table-bordered", escape=False)}
    """
    return dedent(message)

for rank in ranks:
    df_rank = df[df[rank]]
    message = build_html_message(rank, df_rank)
    # save string to rank.html
    with open(f"{rank}.html", "w") as file:
        file.write(message)
# %%
# get the email addresses of the leaders
leaders = pd.read_csv("../leaders.csv")
leaders = leaders[["Email", "Rank Leader"]]
# %%
print("Lion")
print(';'.join(leaders[leaders["Rank Leader"] == "Lion"]["Email"].tolist()))
print("Tiger")
print(';'.join(leaders[leaders["Rank Leader"] == "Tiger"]["Email"].tolist()))
print("Wolf")
print(';'.join(leaders[leaders["Rank Leader"] == "Wolf"]["Email"].tolist()))
print("Bear")
print(';'.join(leaders[leaders["Rank Leader"] == "Bear"]["Email"].tolist()))
print("Webelos")
print(';'.join(leaders[leaders["Rank Leader"] == "Webelos"]["Email"].tolist()))
# %%
