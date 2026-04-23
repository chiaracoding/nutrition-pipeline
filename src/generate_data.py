"""
generate_data.py
----------------
Generates synthetic nutrition coaching data for ONE batch week.
DIsclaimer: This part of the project was developed with the support of Claude (Anthropic) as an AI coding assistant.
All code has been reviewed, tested, and understood by the author.

Simulation logic:
  - In a real system, each week a new foodlog CSV would be uploaded
  - This script simulates that: it generates exactly one week of data
  - The pipeline processes this one file and outputs weekly reports per client and a company report for coaching performance

Scale:
  - 40,000 clients  →  ~1,800,000 food log entries for the week

Deliberate raw data imperfections:
  - clients.csv has ~4% missing values in activity_level and height_cm
  - daily_calorie_target is NOT in raw data but calculated in processing microservice
  - foodlog_week.csv has ~1% duplicate entries (double submissions)
  - foodlog_week.csv has ~10% of rows with macro/calorie mismatch
  - Calories are realistic: meals 400-800 kcal, daily total ~1800-2500 kcal

Produces:
  data/source/clients.csv           (40,000 users, raw registration data)
  data/source/foodlog_week.csv      (~1.8M entries, this week's food logs)
"""

import pandas as pd
import numpy as np
import os
import random
from datetime import datetime, timedelta

# ── Reproducibility ───────────────────────────────────────────────────────────
SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# ── Output paths ──────────────────────────────────────────────────────────────
os.makedirs("data/source", exist_ok=True)
CLIENTS_PATH = "data/source/clients.csv"
FOODLOG_PATH = "data/source/foodlog_week.csv"

# ── Configuration ─────────────────────────────────────────────────────────────
NUM_CLIENTS = 40_000
WEEK_START  = datetime(2024, 1, 1)   # Monday — change for next week's run

# ── Food reference data ───────────────────────────────────────────────────────
# Calories are per realistic MEAL PORTION (not per 100g)
# Macros are correct: protein*4 + carbs*4 + fat*9 ≈ calories
FOOD_ITEMS = [
    # (food_name,               calories, protein_g, carbs_g, fat_g)
    # Breakfast items — realistic full meal portions
    ("Oatmeal with milk",            520,      16,      78,    14),
    ("Scrambled eggs (3) + toast",   620,      32,      44,    30),
    ("Greek yogurt + granola",       580,      24,      76,    16),
    ("Banana smoothie",              480,      12,      88,     9),
    ("Avocado toast (2 slices)",     560,      15,      52,    30),
    ("Protein pancakes",             640,      38,      70,    18),
    # Lunch items
    ("Chicken breast + rice",        720,      58,      72,    10),
    ("Tuna sandwich",                620,      42,      58,    18),
    ("Lentil soup + bread",          640,      28,      96,    10),
    ("Caesar salad + chicken",       680,      50,      28,    36),
    ("Pasta bolognese",              820,      38,     104,    22),
    ("Veggie wrap",                  580,      18,      78,    18),
    # Dinner items
    ("Salmon + sweet potato",        760,      56,      58,    22),
    ("Beef steak + vegetables",      840,      68,      28,    42),
    ("Chicken stir fry + rice",      720,      50,      76,    14),
    ("Vegetable curry + rice",       660,      18,     104,    16),
    ("Turkey meatballs + pasta",     780,      50,      82,    20),
    ("Grilled cod + salad",          540,      50,      24,    18),
    # Snacks
    ("Almonds (30g)",                180,       6,       6,    16),
    ("Protein shake",                280,      36,      22,     5),
    ("Apple + peanut butter",        320,       8,      44,    14),
    ("Cottage cheese + berries",     260,      22,      28,     5),
    ("Rice cakes + hummus",          280,      10,      44,     7),
    ("Mixed nuts (40g)",             260,       8,      10,    23),
    ("Banana",                       120,       2,      30,     0),
]

MEAL_HOURS = {
    "breakfast": 8,
    "lunch":     13,
    "dinner":    19,
    "snack":     16,
}

ACTIVITY_LEVELS = ["sedentary", "light", "moderate", "active"]
ACTIVITY_PROBS  = [0.20, 0.30, 0.30, 0.20]
GOALS           = ["weight_loss", "muscle_gain", "maintenance"]

# Breakfast foods (indices 0-5), lunch (6-11), dinner (12-17), snack (18-24)
MEAL_FOOD_MAP = {
    "breakfast": FOOD_ITEMS[0:6],
    "lunch":     FOOD_ITEMS[6:12],
    "dinner":    FOOD_ITEMS[12:18],
    "snack":     FOOD_ITEMS[18:25],
}


# ─────────────────────────────────────────────────────────────────────────────
# 1. GENERATE CLIENTS  — raw registration data, no derived fields
# ─────────────────────────────────────────────────────────────────────────────
print("Generating clients...")

client_ids = [f"U{str(i).zfill(6)}" for i in range(1, NUM_CLIENTS + 1)]

# Join dates spread realistically over 18 months before the batch week
join_dates = [
    (WEEK_START - timedelta(days=random.randint(30, 540))).strftime("%Y-%m-%d")
    for _ in range(NUM_CLIENTS)
]

ages       = np.random.randint(18, 65,  size=NUM_CLIENTS)
genders    = np.random.choice(["male", "female"], size=NUM_CLIENTS)
weights    = np.round(np.random.uniform(55, 110, size=NUM_CLIENTS), 1)
heights    = np.random.randint(155, 195, size=NUM_CLIENTS).astype(float)
goals      = np.random.choice(GOALS, size=NUM_CLIENTS, p=[0.50, 0.30, 0.20])
activity   = np.random.choice(ACTIVITY_LEVELS, size=NUM_CLIENTS, p=ACTIVITY_PROBS)

clients = pd.DataFrame({
    "user_id":        client_ids,
    "age":            ages,
    "gender":         genders,
    "weight_kg":      weights,
    "height_cm":      heights,
    "goal":           goals,
    "activity_level": activity,
    "join_date":      join_dates,
})

# ── Introduce realistic missing values (~4%) ──────────────────────────────────
# ~2% missing height_cm  (user skipped the field during registration)
height_missing = np.random.choice(NUM_CLIENTS,
                                   size=int(NUM_CLIENTS * 0.02), replace=False)
clients.loc[height_missing, "height_cm"] = np.nan

# ~2% missing activity_level  (user never completed their profile)
activity_missing = np.random.choice(NUM_CLIENTS,
                                     size=int(NUM_CLIENTS * 0.02), replace=False)
clients.loc[activity_missing, "activity_level"] = np.nan

# NOTE: daily_calorie_target is intentionally NOT here.
# It will be calculated in the processing step using Harris-Benedict formula.

clients.to_csv(CLIENTS_PATH, index=False)

missing_h = clients["height_cm"].isna().sum()
missing_a = clients["activity_level"].isna().sum()
print(f"  ✓ {len(clients):,} clients saved to {CLIENTS_PATH}")
print(f"  ✓ Missing height_cm:      {missing_h} rows ({missing_h/NUM_CLIENTS*100:.1f}%)")
print(f"  ✓ Missing activity_level: {missing_a} rows ({missing_a/NUM_CLIENTS*100:.1f}%)")
print(f"  ✓ Join dates spread: {clients['join_date'].min()} → {clients['join_date'].max()}")
print(f"  ✓ No daily_calorie_target column — calculated in processing step")


# ─────────────────────────────────────────────────────────────────────────────
# 2. GENERATE ONE WEEK OF FOOD LOGS
# ─────────────────────────────────────────────────────────────────────────────
week_end = (WEEK_START + timedelta(days=6)).strftime("%Y-%m-%d")
print(f"\nGenerating food log for {WEEK_START.date()} → {week_end}...")
print("  (This may take ~1 minute...)\n")

records     = []
log_counter = 1

for day_offset in range(7):
    log_date    = WEEK_START + timedelta(days=day_offset)
    day_records = 0

    for user_id in client_ids:
        num_meals    = random.choices([3, 4], weights=[0.6, 0.4])[0]
        todays_meals = ["breakfast", "lunch", "dinner"]
        if num_meals == 4:
            todays_meals.append("snack")

        for meal_type in todays_meals:
            # Pick 1 food item per meal from the correct meal category
            food_pool = MEAL_FOOD_MAP[meal_type]
            food      = random.choice(food_pool)
            food_name, kcal, protein, carbs, fat = food

            # Realistic portion variation ±12%
            variation     = np.random.uniform(0.88, 1.12)
            protein_final = round(protein * variation, 1)
            carbs_final   = round(carbs   * variation, 1)
            fat_final     = round(fat     * variation, 1)

            # For 90% of rows: calories are DERIVED from macros → always consistent
            # protein*4 + carbs*4 + fat*9 = total calories (standard formula)
            cal_final = round(
                protein_final * 4 + carbs_final * 4 + fat_final * 9, 1
            )

            # ── Introduce macro/calorie mismatch in ~10% of rows ─────────────
            # Simulates app sync errors or manual entry mistakes
            # (e.g. user manually typed calories wrong, or app logged incorrectly)
            if random.random() < 0.10:
                cal_final = round(cal_final * np.random.uniform(0.75, 1.30), 1)

            minute = random.randint(0, 45)
            ts     = log_date.replace(hour=MEAL_HOURS[meal_type], minute=minute)

            records.append((
                f"L{str(log_counter).zfill(10)}",
                user_id,
                ts.strftime("%Y-%m-%d %H:%M:%S"),
                meal_type,
                food_name,
                cal_final,
                protein_final,
                carbs_final,
                fat_final,
            ))
            log_counter += 1
            day_records += 1

    print(f"  {log_date.strftime('%A %Y-%m-%d')}: {day_records:>10,} entries")

foodlog = pd.DataFrame(records, columns=[
    "log_id", "user_id", "timestamp",
    "meal_type", "food_item",
    "calories", "protein_g", "carbs_g", "fat_g"
])

# ── Introduce ~1% duplicate entries ──────────────────────────────────────────
# Simulates users accidentally submitting the same meal twice
n_dupes = int(len(foodlog) * 0.01)
dupe_idx = np.random.choice(len(foodlog), size=n_dupes, replace=False)
dupes    = foodlog.iloc[dupe_idx].copy()
dupes["log_id"] = [f"L{str(log_counter + i).zfill(10)}"
                   for i in range(len(dupes))]
foodlog  = pd.concat([foodlog, dupes], ignore_index=True)

# Sort by timestamp so it looks like a natural log
foodlog = foodlog.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

foodlog.to_csv(FOODLOG_PATH, index=False)

# ─────────────────────────────────────────────────────────────────────────────
# 3. SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
# Quick calorie check
foodlog["date"] = pd.to_datetime(foodlog["timestamp"]).dt.date
daily_kcal = foodlog.groupby(["user_id", "date"])["calories"].sum()

# Macro consistency check
foodlog["calc_kcal"] = (foodlog["protein_g"] * 4 +
                        foodlog["carbs_g"]   * 4 +
                        foodlog["fat_g"]     * 9)
foodlog["macro_diff"] = (foodlog["calories"] - foodlog["calc_kcal"]).abs()
mismatch_pct = (foodlog["macro_diff"] > 20).mean() * 100

print(f"\n── Data Generation Complete ──────────────────────────────────────")
print(f"  clients.csv       : {len(clients):>10,} rows")
print(f"  foodlog_week.csv  : {len(foodlog):>10,} rows  (incl. ~1% dupes)")
print(f"  Week covered      : {WEEK_START.date()} → {week_end}")
print(f"\n  Calorie realism check (daily total per user):")
print(f"    Mean  : {daily_kcal.mean():>8.0f} kcal/day")
print(f"    Min   : {daily_kcal.min():>8.0f} kcal/day")
print(f"    Max   : {daily_kcal.max():>8.0f} kcal/day")
print(f"\n  Data quality issues introduced (for processing step to handle):")
print(f"    Missing height_cm       : {missing_h:>6,} rows ({missing_h/NUM_CLIENTS*100:.1f}%)")
print(f"    Missing activity_level  : {missing_a:>6,} rows ({missing_a/NUM_CLIENTS*100:.1f}%)")
print(f"    Duplicate log entries   : ~{n_dupes:>5,} rows (~1.0%)")
print(f"    Macro/calorie mismatch  : {mismatch_pct:>7.1f}% of rows")
print(f"\n  Seed : {SEED}  (fully reproducible)")
print(f"  → Next step: run ingestion.py")
print(f"────────────────────────────────────────────────────────────────────")