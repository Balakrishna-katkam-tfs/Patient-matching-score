# import polars as pl
# from datetime import date

# # ================================================================
# # STEP 1. LOAD CSVs
# # ================================================================
# csv1 = pl.read_csv(
#     "C:\\Thermo_fisher\\Match_score\\AI_Tagged_Synthetic_Data_Final 2_flat.csv",
#     infer_schema_length=2000,
#     ignore_errors=True
# )

# csv2 = pl.read_csv(
#     "EDW_DATA_4_ML.csv",
#     infer_schema_length=2000,
#     ignore_errors=True
# )

# # Cast IDs to strings for join safety
# csv1 = csv1.with_columns(pl.col("PATIENT_ID").cast(pl.Utf8))
# csv2 = csv2.with_columns(pl.col("PATIENT_ID").cast(pl.Utf8))

# # ================================================================
# # STEP 2. MERGE BOTH DATASETS
# # ================================================================
# merged = csv1.join(csv2, on=["PATIENT_ID", "STUDY_ID"], how="full")

# # ================================================================
# # STEP 3. STANDARDIZE CORE COLUMNS
# # ================================================================
# if "SEX" in merged.columns:
#     merged = merged.with_columns(pl.col("SEX").cast(pl.Utf8).alias("sex"))
# elif "GENDER" in merged.columns:
#     merged = merged.with_columns(pl.col("GENDER").cast(pl.Utf8).alias("sex"))
# else:
#     merged = merged.with_columns(pl.lit(None).alias("sex"))

# if "STUDY_ID" in merged.columns:
#     merged = merged.with_columns(pl.col("STUDY_ID").alias("study_id"))
# elif "STUDY_ID_right" in merged.columns:
#     merged = merged.with_columns(pl.col("STUDY_ID_right").alias("study_id"))

# # ================================================================
# # STEP 4. AGE CALCULATION (no scoring applied)
# # ================================================================
# dob_candidates = [c for c in merged.columns if c.strip().upper() in ["DOB", "DATE_OF_BIRTH"]]
# if dob_candidates:
#     dob_col = dob_candidates[0]
#     formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S.%f"]

#     parsed_cols = []
#     for i, fmt in enumerate(formats):
#         parsed_cols.append(
#             pl.col(dob_col)
#             .cast(pl.Utf8)
#             .str.replace_all(r"Z$", "")
#             .str.strip_chars()
#             .str.strptime(pl.Date, fmt, strict=False)
#             .alias(f"dob_{i}")
#         )

#     merged = merged.with_columns(parsed_cols)
#     merged = merged.with_columns(
#         pl.coalesce([pl.col(c) for c in merged.columns if c.startswith("dob_")]).alias("dob_parsed")
#     )

#     today = date.today()
#     merged = merged.with_columns(
#         ((pl.lit(today.year) - pl.col("dob_parsed").dt.year()) -
#          ((pl.lit(today.month) < pl.col("dob_parsed").dt.month()) |
#           ((pl.lit(today.month) == pl.col("dob_parsed").dt.month()) &
#            (pl.lit(today.day) < pl.col("dob_parsed").dt.day()))).cast(pl.Int64))
#         .alias("age")
#     )

#     merged = merged.drop([c for c in merged.columns if c.startswith("dob_")])

# # ================================================================
# # STEP 5. DIAGNOSIS RECENCY
# # ================================================================
# if "DIAGNOSES.temporal_info.time_value_lower_limit" in merged.columns:
#     merged = merged.with_columns(
#         pl.col("DIAGNOSES.temporal_info.time_value_lower_limit").cast(pl.Utf8)
#         .str.replace_all("Y", "")
#         .cast(pl.Float64)
#         .alias("diagnosis_years")
#     )

# # Recency scoring (≤5 yrs = 50 max, 10 pts per year)
# merged = merged.with_columns(
#     pl.when(pl.col("diagnosis_years") <= 5)
#     .then((5 - pl.col("diagnosis_years")) * 10)
#     .otherwise(0)
#     .alias("recency_points")
# )

# # ================================================================
# # STEP 6. PPD SCREENING SCORE
# # ================================================================
# ppd_df = (
#     merged
#     .group_by("PATIENT_ID")
#     .agg([
#         (pl.col("ACTIVITY_CATEGORY")
#            .filter(pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS").count()
#         ).alias("qualified_flag"),
#         (pl.col("ACTIVITY_CATEGORY")
#            .filter(pl.col("ACTIVITY_CATEGORY") == "RESPONDENTS").count()
#         ).alias("respondent_flag")
#     ])
#     .with_columns(
#         pl.when(pl.col("qualified_flag") > 0).then(pl.lit(30))
#          .when(pl.col("respondent_flag") > 0).then(pl.lit(20))
#          .otherwise(pl.lit(0))
#          .alias("ppd_points")
#     )
# )

# # ================================================================
# # STEP 7. SIMILAR STUDIES (Neil’s Rule)
# # ================================================================
# sim_df = (
#     merged
#     .filter(pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS")
#     .select(["PATIENT_ID", "INDICATION_NAME"])
#     .drop_nulls()
#     .unique()
#     .group_by("PATIENT_ID")
#     .agg(pl.count("INDICATION_NAME").alias("similar_study_count"))
# )

# # ================================================================
# # STEP 8. PAST QUALIFICATION
# # ================================================================
# past_df = (
#     merged
#     .with_columns(
#         pl.col("ACTIVITY_CATEGORY").is_in(["RANDOMIZATION"]).cast(pl.Int64).alias("randomization_flag")
#     )
#     .group_by("PATIENT_ID")
#     .agg(pl.col("randomization_flag").max().alias("past_qualified"))
# )

# # ================================================================
# # STEP 9. SMS RESPONSE
# # ================================================================
# if "SMS" in merged.columns:
#     sms_df = (
#         merged
#         .with_columns((pl.col("SMS").cast(pl.Utf8).str.to_uppercase() == "YES").cast(pl.Int64).alias("sms_response"))
#         .group_by("PATIENT_ID")
#         .agg(pl.col("sms_response").max().alias("sms_response"))
#     )
# else:
#     sms_df = merged.select(["PATIENT_ID"]).with_columns(pl.lit(0).alias("sms_response"))

# # ================================================================
# # STEP 10. MILESTONE STAGING
# # ================================================================
# stage_order = ["RELEASED", "RESPONDENTS", "REFERRAL", "QUALIFIED RESPONDENTS",
#                "FOV", "FOV SCHEDULED", "CONSENT", "RANDOMIZATION"]
# stage_map = {s: i for i, s in enumerate(stage_order, start=1)}

# milestone_df = (
#     merged
#     .with_columns(
#         pl.col("ACTIVITY_CATEGORY").map_elements(lambda x: stage_map.get(x, 0), return_dtype=pl.Int64).alias("stage_score")
#     )
#     .group_by("PATIENT_ID")
#     .agg([
#         pl.col("stage_score").max().alias("latest_stage_score"),
#         pl.col("ACTIVITY_CATEGORY").first().alias("latest_milestone"),
#         pl.col("study_id").first().alias("study_id"),
#         pl.col("INDICATION_NAME").first().alias("indication")
#     ])
# )

# # ================================================================
# # STEP 11. PATIENT AGGREGATION
# # ================================================================
# patient_df = merged.select(["PATIENT_ID", "sex"]).unique()

# for df in [ppd_df, sim_df, past_df, sms_df, milestone_df]:
#     patient_df = patient_df.join(df, on="PATIENT_ID", how="left")

# if "age" in merged.columns:
#     patient_df = patient_df.join(
#         merged.select(["PATIENT_ID", "age"]).drop_nulls().unique(), on="PATIENT_ID", how="left"
#     )

# if "recency_points" in merged.columns:
#     patient_df = patient_df.join(
#         merged.select(["PATIENT_ID", "recency_points"]).unique(), on="PATIENT_ID", how="left"
#     )

# # Placeholder for Distance
# patient_df = patient_df.with_columns(pl.lit(999).alias("distance_to_site_km"))

# # ================================================================
# # STEP 12. WEIGHTED SCORING FUNCTION (AGE REMOVED)
# # ================================================================
# def compute_weighted_score(row: dict):
#     score = 0
#     ds = row.get("recency_points", 0)
#     act = row.get("latest_milestone", "")
#     sms = row.get("sms_response", 0)
#     dist = row.get("distance_to_site_km", 999)

#     # Recency
#     score += int(ds or 0)

#     # PPD
#     ppd = row.get("ppd_points") or 0
#     score += int(ppd)

#     # Similar studies (Qualified Indications)
#     if row.get("similar_study_count"):
#         score += int(row["similar_study_count"]) * 20

#     # SMS response (<3 years)
#     if sms == 1:
#         if ds <= 1095:
#             score += 25

#     # Distance scoring
#     if dist < 10:
#         score += 20
#     elif dist <= 50:
#         score += 15
#     else:
#         score += 10

#     # Past qualification (exclude released)
#     if act == "RANDOMIZATION" and ds:
#         if ds > 365:
#             score += 25
#         elif ds > 180:
#             score += 15
#     elif act == "RELEASED":
#         score += 0

#     return score

# # ================================================================
# # STEP 13. APPLY SCORING
# # ================================================================
# patient_df = patient_df.with_columns(
#     pl.struct(patient_df.columns).map_elements(compute_weighted_score, return_dtype=pl.Int64).alias("business_score")
# )

# # ================================================================
# # STEP 14. NORMALIZE
# # ================================================================
# min_score, max_score = patient_df["business_score"].min(), patient_df["business_score"].max()
# patient_df = patient_df.with_columns(
#     ((pl.col("business_score") - min_score) / (max_score - min_score)).alias("business_score_normalized")
# )

# # ================================================================
# # STEP 15. SAVE OUTPUT
# # ================================================================
# patient_df.write_csv("patient_scores_final_v22222.csv")
# print("✅ Final dataset created:", patient_df.shape)

import polars as pl
from datetime import date, datetime
import pgeocode
from tqdm import tqdm

# ================================================================
# STEP 1. LOAD CSVs
# ================================================================
csv1 = pl.read_csv(
    "C:\\Thermo_fisher\\Match_score\\AI_Tagged_Synthetic_Data_Final 2_flat.csv",
    infer_schema_length=2000,
    ignore_errors=True
)

csv2 = pl.read_csv(
    "C:\\Thermo_fisher\\Match_score\\EDW_DATA_4_ML.csv",
    infer_schema_length=2000,
    ignore_errors=True
)

# Cast IDs to strings for join safety
csv1 = csv1.with_columns(pl.col("PATIENT_ID").cast(pl.Utf8))
csv2 = csv2.with_columns(pl.col("PATIENT_ID").cast(pl.Utf8))

# ================================================================
# STEP 2. MERGE BOTH DATASETS
# ================================================================
merged = csv1.join(csv2, on=["PATIENT_ID", "STUDY_ID"], how="full")

# ================================================================
# STEP 3. STANDARDIZE CORE COLUMNS
# ================================================================
if "SEX" in merged.columns:
    merged = merged.with_columns(pl.col("SEX").cast(pl.Utf8).alias("sex"))
elif "GENDER" in merged.columns:
    merged = merged.with_columns(pl.col("GENDER").cast(pl.Utf8).alias("sex"))
else:
    merged = merged.with_columns(pl.lit(None).alias("sex"))

if "STUDY_ID" in merged.columns:
    merged = merged.with_columns(pl.col("STUDY_ID").alias("study_id"))
elif "STUDY_ID_right" in merged.columns:
    merged = merged.with_columns(pl.col("STUDY_ID_right").alias("study_id"))

# ================================================================
# STEP 4. AGE CALCULATION (no scoring applied)
# ================================================================
dob_candidates = [c for c in merged.columns if c.strip().upper() in ["DOB", "DATE_OF_BIRTH"]]
if dob_candidates:
    dob_col = dob_candidates[0]
    formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %.f"]

    parsed_cols = []
    for i, fmt in enumerate(formats):
        parsed_cols.append(
            pl.col(dob_col)
            .cast(pl.Utf8)
            .str.replace_all(r"Z$", "")
            .str.strip_chars()
            .str.strptime(pl.Date, fmt, strict=False)
            .alias(f"dob_{i}")
        )

    merged = merged.with_columns(parsed_cols)
    merged = merged.with_columns(
        pl.coalesce([pl.col(c) for c in merged.columns if c.startswith("dob_")]).alias("dob_parsed")
    )

    today = date.today()
    merged = merged.with_columns(
        ((pl.lit(today.year) - pl.col("dob_parsed").dt.year()) -
         ((pl.lit(today.month) < pl.col("dob_parsed").dt.month()) |
          ((pl.lit(today.month) == pl.col("dob_parsed").dt.month()) &
           (pl.lit(today.day) < pl.col("dob_parsed").dt.day()))).cast(pl.Int64))
        .alias("age")
    )

    merged = merged.drop([c for c in merged.columns if c.startswith("dob_")])

# ================================================================
# STEP 5. DIAGNOSIS RECENCY (Primary + Fallback from Qualified Respondents)
# ================================================================
today = datetime.today()

# Primary: from DIAGNOSES.temporal_info.time_value_lower_limit (csv1)
if "DIAGNOSES.temporal_info.time_value_lower_limit" in merged.columns:
    merged = merged.with_columns(
        pl.col("DIAGNOSES.temporal_info.time_value_lower_limit")
        .cast(pl.Utf8)
        .str.replace_all("Y", "")
        .cast(pl.Float64)
        .alias("diagnosis_years")
    )

# Step 5a: Compute recency from diagnosis (primary)
merged = merged.with_columns(
    pl.when(pl.col("diagnosis_years").is_not_null())
    .then(
        pl.when(pl.col("diagnosis_years") <= 5)
        .then((5 - pl.col("diagnosis_years")) * 10)
        .otherwise(0)
    )
    .otherwise(pl.lit(None))
    .alias("recency_points_diagnosis")
)

# Step 5b: Fallback — use MOST RECENT ACTIVITY_DATE from EDW_DATA_4_ML.csv (any activity)
if "ACTIVITY_DATE" in merged.columns:
    # Parse all activity dates safely
    merged = merged.with_columns([
        pl.coalesce([
            pl.col("ACTIVITY_DATE").cast(pl.Utf8).str.strptime(pl.Date, "%m/%d/%Y", strict=False),
            pl.col("ACTIVITY_DATE").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        ]).alias("activity_date_parsed")
    ])

    # Get most recent activity date per patient (from EDW_DATA_4_ML.csv)
    activity_recency = (
        merged
        .filter(pl.col("activity_date_parsed").is_not_null())
        .group_by("PATIENT_ID")
        .agg([
            pl.col("activity_date_parsed").max().alias("most_recent_activity_date"),
            (pl.lit(today.date()) - pl.col("activity_date_parsed").max())
            .dt.total_days()
            .alias("days_since_last_activity")
        ])
        .with_columns(
            # Convert days to recency points (10 points per year, max 5 years)
            pl.when(pl.col("days_since_last_activity") <= 1825)  # 5 years
            .then(((1825 - pl.col("days_since_last_activity")) / 365.25 * 10).cast(pl.Int64))
            .otherwise(0)
            .alias("recency_points_fallback")
        )
    )

    merged = merged.join(activity_recency, on="PATIENT_ID", how="left")

    # Remove any duplicate 'recency_points' if exists
    for col in ["recency_points", "recency_points_final"]:
        if col in merged.columns:
            merged = merged.drop(col)

    # Combine both sources: Primary (diagnosis) takes precedence, fallback to recent activity
    merged = merged.with_columns(
        pl.when(pl.col("recency_points_diagnosis").is_not_null())
        .then(pl.col("recency_points_diagnosis"))
        .when(pl.col("recency_points_fallback").is_not_null())
        .then(pl.col("recency_points_fallback"))
        .otherwise(0)
        .alias("recency_points")
    )

    # Add transparency for debugging
    merged = merged.with_columns(
        pl.when(pl.col("recency_points_diagnosis").is_not_null())
        .then(pl.lit("Diagnosis-based"))
        .when(pl.col("recency_points_fallback").is_not_null())
        .then(pl.lit("Recent Activity-based"))
        .otherwise(pl.lit("No Recency Info"))
        .alias("recency_reason")
    )

# ================================================================
# STEP 6. PPD SCREENING SCORE
# ================================================================
ppd_df = (
    merged
    .group_by("PATIENT_ID")
    .agg([
        (pl.col("ACTIVITY_CATEGORY")
           .filter(pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS").count()
        ).alias("qualified_flag"),
        (pl.col("ACTIVITY_CATEGORY")
           .filter(pl.col("ACTIVITY_CATEGORY") == "RESPONDENTS").count()
        ).alias("respondent_flag")
    ])
    .with_columns(
        pl.when(pl.col("qualified_flag") > 0).then(pl.lit(30))
         .when(pl.col("respondent_flag") > 0).then(pl.lit(20))
         .otherwise(pl.lit(0))
         .alias("ppd_points")
    )
)

# ================================================================
# STEP 7. SIMILAR STUDIES (Neil’s Rule)
# ================================================================
sim_df = (
    merged
    .filter(pl.col("ACTIVITY_CATEGORY") == "QUALIFIED RESPONDENTS")
    .select(["PATIENT_ID", "INDICATION_NAME"])
    .drop_nulls()
    .unique()
    .group_by("PATIENT_ID")
    .agg(pl.count("INDICATION_NAME").alias("similar_study_count"))
)

# ================================================================
# STEP 8. PAST QUALIFICATION
# ================================================================
past_df = (
    merged
    .with_columns(
        pl.col("ACTIVITY_CATEGORY").is_in(["RANDOMIZATION"]).cast(pl.Int64).alias("randomization_flag")
    )
    .group_by("PATIENT_ID")
    .agg(pl.col("randomization_flag").max().alias("past_qualified"))
)

# ================================================================
# STEP 9. SMS RESPONSE
# ================================================================
if "SMS" in merged.columns:
    sms_df = (
        merged
        .with_columns((pl.col("SMS").cast(pl.Utf8).str.to_uppercase() == "YES").cast(pl.Int64).alias("sms_response"))
        .group_by("PATIENT_ID")
        .agg(pl.col("sms_response").max().alias("sms_response"))
    )
else:
    sms_df = merged.select(["PATIENT_ID"]).with_columns(pl.lit(0).alias("sms_response"))

# ================================================================
# STEP 10. MILESTONE STAGING
# ================================================================
stage_order = ["RELEASED", "RESPONDENTS", "REFERRAL", "QUALIFIED RESPONDENTS",
               "FOV", "FOV SCHEDULED", "CONSENT", "RANDOMIZATION"]
stage_map = {s: i for i, s in enumerate(stage_order, start=1)}

milestone_df = (
    merged
    .with_columns(
        pl.col("ACTIVITY_CATEGORY").map_elements(lambda x: stage_map.get(x, 0), return_dtype=pl.Int64).alias("stage_score")
    )
    .group_by("PATIENT_ID")
    .agg([
        pl.col("stage_score").max().alias("latest_stage_score"),
        pl.col("ACTIVITY_CATEGORY").first().alias("latest_milestone"),
        pl.col("study_id").first().alias("study_id"),
        pl.col("INDICATION_NAME").first().alias("indication")
    ])
)

# ================================================================
# STEP 11. PATIENT AGGREGATION
# ================================================================
patient_df = merged.select(["PATIENT_ID", "sex"]).unique()

for df in [ppd_df, sim_df, past_df, sms_df, milestone_df]:
    patient_df = patient_df.join(df, on="PATIENT_ID", how="left")

if "age" in merged.columns:
    patient_df = patient_df.join(
        merged.select(["PATIENT_ID", "age"]).drop_nulls().unique(), on="PATIENT_ID", how="left"
    )

if "recency_points" in merged.columns:
    patient_df = patient_df.join(
        merged.select(["PATIENT_ID", "recency_points", "recency_reason"]).unique(), on="PATIENT_ID", how="left"
    )

# ================================================================
# STEP 11.5. DISTANCE CALCULATION
# ================================================================
nomi = pgeocode.Nominatim('us')

def calculate_distance_to_closest_site(patient_zip, site_zips=['10001', '90210', '60601']):
    """Calculate distance to closest site from patient zip code"""
    if not patient_zip or patient_zip == 'Unknown':
        return 999
    
    try:
        patient_info = nomi.query_postal_code(str(patient_zip))
        if patient_info.latitude is None:
            return 999
        
        min_distance = float('inf')
        for site_zip in site_zips:
            site_info = nomi.query_postal_code(str(site_zip))
            if site_info.latitude is not None:
                distance = pgeocode.haversine_distance(
                    (patient_info.latitude, patient_info.longitude),
                    (site_info.latitude, site_info.longitude)
                )
                min_distance = min(min_distance, distance)
        
        return min_distance if min_distance != float('inf') else 999
    except:
        return 999

def calculate_distances_with_progress(zip_codes):
    """Calculate distances with progress bar"""
    distances = []
    for zip_code in tqdm(zip_codes, desc="Calculating distances"):
        distances.append(calculate_distance_to_closest_site(zip_code))
    return distances

# Keep distance as placeholder since we don't have actual trial site locations
# Real distances will be calculated in API when users provide site zip codes
patient_df = patient_df.with_columns(pl.lit(999).alias("distance_to_site_km"))

# ================================================================
# STEP 12. WEIGHTED SCORING FUNCTION (AGE REMOVED)
# ================================================================
def compute_weighted_score(row: dict):
    score = 0
    ds = row.get("recency_points", 0)
    act = row.get("latest_milestone", "")
    sms = row.get("sms_response", 0)
    dist = row.get("distance_to_site_km", 999)

    # Recency
    score += int(ds or 0)

    # PPD
    ppd = row.get("ppd_points") or 0
    score += int(ppd)

    # Similar studies
    if row.get("similar_study_count"):
        score += int(row["similar_study_count"]) * 20

    # SMS response (<3 years)
    if sms == 1 and ds <= 1095:
        score += 25

    # Distance scoring
    if dist < 10:
        score += 20
    elif dist <= 50:
        score += 15
    else:
        score += 10

    # Past qualification (exclude released)
    if act == "RANDOMIZATION" and ds:
        if ds > 365:
            score += 25
        elif ds > 180:
            score += 15
    elif act == "RELEASED":
        score += 0

    return score

# ================================================================
# STEP 13. APPLY SCORING
# ================================================================
patient_df = patient_df.with_columns(
    pl.struct(patient_df.columns).map_elements(compute_weighted_score, return_dtype=pl.Int64).alias("business_score")
)

# ================================================================
# STEP 14. NORMALIZE
# ================================================================
min_score, max_score = patient_df["business_score"].min(), patient_df["business_score"].max()
patient_df = patient_df.with_columns(
    ((pl.col("business_score") - min_score) / (max_score - min_score)).alias("business_score_normalized")
)

# ================================================================
# STEP 15. SAVE OUTPUT
# ================================================================
output_path = "patient_scores_final_v5555.csv"
patient_df.write_csv(output_path)
print(f"Final dataset created: {patient_df.shape} -> {output_path}")
