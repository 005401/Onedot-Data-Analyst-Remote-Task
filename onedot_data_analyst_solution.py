#!/usr/bin/env python
# coding: utf-8 

import pandas as pd
import json

input_data = 'supplier_car.json'

#read input json file, take care of the encoding!
d = [json.loads(line) for line in open(input_data, 'r', encoding='utf-8')]
df = pd.DataFrame(data=d)

#convert ID column to numeric type and sort it
df['ID'] = df['ID'].astype('float')
df = df.sort_values("ID")

# Pre-processing
# I need to transform the supplier data to achieve the same granularity as the target data

# Some rows in the input data have repeting ID and column values, while only the Attribute Names and Attribute Values change.
# We can group them and then make new columns with all Attribute Names and Attribute Values in single row for each ID
df_grp = df.groupby(["ID", "MakeText", "TypeName", "TypeNameFull", "ModelText", "ModelTypeText"], dropna=False)
df_grp_agg = df_grp.agg({"Attribute Names": list, "Attribute Values": list}).reset_index()

# some ModelText vaues are NaN due to dropna=False above
# the line below helps with normalization later
df_grp_agg['ModelText'] = df_grp_agg['ModelText'].astype('str')

# define function to aggragate attributes
def aggr_attr(row):
    attr = {key: value for key, value in zip(row["Attribute Names"], row["Attribute Values"])}
    attr.update({col_name: row[col_name] for col_name in ["ID", "MakeText", "TypeName", "TypeNameFull", "ModelText", "ModelTypeText"]})
    return pd.Series(attr)

# Perform aggragate
df_grp_agg_attr = df_grp_agg.apply(aggr_attr, axis=1)


# Normalization
# Normalisation is required in case an attribute value is different but actually is the same (different
# spelling, language, different unit used etc.).
# 
# E.g. the first column in the target data "carType" defines car body Types (Coupé	Convertible / Roadster	Other etc.)
# The column BodyTypeText in the input data can be used as carTypes for the target data 
# but it uses slightly different names (some are in German) and some are missgng (e.g. Single seater)
# We could use the number of seats to find the "Single seater" cars for the target data

# Normalize column "carType"
# it is neccessary to change the names of the car types found in BodyTypeText column of the input data to match
# the names used in Target Data carType column and use the number of seats to find the "Single seater". 
# If the BodyType in input data can't be assigned to one of the types in target data, put it to "Other"
def norm_cartype(row):
    if row["Seats"] == '1':
        return "Single seater"
    try:
        return {
            "Coupé": "Coupé",
            "Limousine": "Saloon",
            "Cabriolet": "Convertible / Roadster",
            "Kombi": "Station Wagon",
            "SUV / Geländewagen": "SUV",
        }[row["BodyTypeText"]]
    except KeyError:
        return "Other"

# Normalize column "color"
# the same as with carType. Some colors are in German and some are slightly different. 
# Assign them to "standard" values from target data
def norm_color(row):
    for color_en, color_de in [
            ("Black", ["schwarz"]), ("Silver", ["silber"]), ("Blue", ["blau"]), ("Gray", ["grau", "anthrazit"]), 
            ("White", ["weiss"]), ("Red", ["red", "bordeaux"]), ("Green", ["grün"]), ("Yellow", ["gelb"]), 
            ("Purple", ["violett"]), ("Gold", ["gold"]), ("Brown", ["braun"]), ("Orange", ["orange"]), ("Beige", ["beige"])]:
        for item in color_de:
            if item in row["BodyColorText"]:
                return color_en
    return "Other"

# Normalize column ConditionTypeText
def norm_condition(row):
    try:
        return {
            "Occasion": "Used",
            "Oldtimer": "Restored",
            "Neu": "New",
            "Vorführmodell": "Original Condition",
        }[row["ConditionTypeText"]]
    except KeyError:
        return "Other"
    
# Normalize columns model and model_variant
# In order to extract  variant we can remove  model  from the column ModelTypeText. 
# If this doesn't work, we use TypeName.
def norm_variant(row):
    model = row["ModelText"].strip()
    model_variant = row["ModelTypeText"].strip()
    if model_variant[:len(model)].lower() == model.lower():
        return model_variant[len(model):].strip()
    return row["TypeName"]

# Normalize column Zip
# Can be done automatically with e.g. pgeocode
def norm_zip(row):
    try:
        return {
            "Zuzwil": "9524",
            "Porrentruy": "2900",
            "Sursee": "6210",
            "Safenwil": "5745",
            "Basel": "4000",
            "St. Galen": "9000",
        }[row["City"]]
    except KeyError:
        return "Other"


# Perform normalization of all columns posible to normalize
NORM_FUNCT = {
    "BodyTypeText": norm_cartype,
    "BodyColorText": norm_color,
    "Condition": norm_condition,
    "Variant": norm_variant,
    "Zip": norm_zip,
}

def normalize(row):
    for column, funct in NORM_FUNCT.items():
        row[column] = funct(row)
    return row
    
# perform normalization    
normalized_df = df_grp_agg_attr.apply(normalize, axis=1)

# Define finction to integrate normalized data into the target data format
def integrate(row):
    return pd.Series({
        "carType": row["BodyTypeText"],
        "color": row["BodyColorText"],
        "condition": row["Condition"],
        "currency": "CHF", # assume that all cars are from/to be sold CH
        "drive": "LHD", # all cars in the input data are from CH, hence LHD, could not find column to normalize
        "city": row["City"], # all cities are from CH
        "country": "CH", # all cars in the input data are from CH but can be deduced from the city name
        "make": row["MakeText"],
        "manufacture_year": row["FirstRegYear"],
        "mileage": row["Km"], 
        "mileage_unit": "kilometer", # all cars are form CH
        "model": row["ModelText"],
        "model_variant": row["Variant"], 
        "price_on_request": None, # could not find column to normalize in the input data
        "type": "car", # all of target data contains the value "car"
        "zip": row["Zip"], # can be inferred from city using e.g pgeocode
        "manufacture_month": row["FirstRegMonth"],
        "fuel_consumption_unit": "l_km_consumption" if row["ConsumptionTotalText"] and row["ConsumptionTotalText"] != 'null' else None,
    })

# perform data integration
integrated_df = normalized_df.apply(integrate, axis=1)

# Write Excel file with three tabls containing results of each step above
with pd.ExcelWriter("onedot_data_analyst_solution.xlsx") as writer:
    df_grp_agg_attr.to_excel(writer, sheet_name="Pre-processed Data", index=False, na_rep="null")
    normalized_df.to_excel(writer, sheet_name="Normalized Data", index=False, na_rep="null")
    integrated_df.to_excel(writer, sheet_name="Integrated Data", index=False, na_rep="null")


