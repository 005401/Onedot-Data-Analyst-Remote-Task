#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import json
from collections import Counter


# ## Take a look at the input data
input_data = 'supplier_car.json'

#read input json file, take care of the encoding!
with open(input_data, encoding='utf-8') as f:
    df = pd.DataFrame([json.loads(line) for line in f.readlines()])

#convert ID column to numeric type and sort it
df.ID = pd.to_numeric(df.ID)
df = df.sort_values("ID")

# ## Pre-processing
# I need to transform the supplier data to achieve the same granularity as the target data

# Some rows in the input data have repeting ID and column values, while only the Attribute Names and Attribute Values change.
# We can group them and then make new columns with all Attribute Names and Attribute Values in single row for each ID

CONSTANT_COLUMNS = ["ID", "MakeText", "TypeName", "TypeNameFull", "ModelText", "ModelTypeText"]

# define function to aggragate attributes
def aggregate_attributes(row):
    attr = {key: value for key, value in zip(row["Attribute Names"], row["Attribute Values"])}
    attr.update({col_name: row[col_name] for col_name in CONSTANT_COLUMNS})
    return pd.Series(attr)

# aggregare input data to create one row per product
agg_data = df.groupby(CONSTANT_COLUMNS).agg(
    {"Attribute Names": list, "Attribute Values": list}
).reset_index().apply(aggregate_attributes, axis=1)



# ## Normalization
# Normalisation is required in case an attribute value is different but actually is the same (different
# spelling, language, different unit used etc.).
# 
# E.g. the first column in the target data "carType" defines cat Types (	Coupé	Convertible / Roadster	Other etc.)
# The column BodyTypeText in the input data can be used as carTypes for the target data 
# but it uses slightly different carType names (some in German) and some are missgng (e.g. Single seater)
# One could use the number of seats to find the "Single seater" cars for the target data


# ### Normalize column "carType"

# it is neccessary to change the names of the car types found in BodyTypeText column of the input data to match
# the names used in Target Data carType column and use the number of seats to find the "Single seater". 
# If the BodyType in input data can't be assigned to one of the types in target data, put it to "Other"

def normalize_cartype(row):
    if row["Seats"] == 1:
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


# ### Normalize column "color"
# the same as with carType. Some color are in German and some are slightly different. Assign them to standard values from target data

def normalize_color(row):
    for english, germans in [
            ("Black", ["schwarz"]), ("Silver", ["silber"]), ("Blue", ["blau"]), ("Gray", ["grau", "anthrazit"]), ("White", ["weiss"]),
            ("Red", ["red", "bordeaux"]), ("Green", ["grün"]), ("Yellow", ["gelb"]), ("Purple", ["violett"]),
            ("Gold", ["gold"]), ("Brown", ["braun"]), ("Orange", ["orange"]), ("Beige", ["beige"])]:
        for german in germans:
            if german in row["BodyColorText"]:
                return english
    return "Other"


# ### Normalize column ConditionTypeText

def normalize_condition(row):
    try:
        return {
            "Occasion": "Used",
            "Oldtimer": "Restored",
            "Neu": "New",
            "Vorführmodell": "Original Condition",
        }[row["ConditionTypeText"]]
    except KeyError:
        return "Other"


# ### Normalize columns model and model_variant
# In order to extract  variant  without  model, we can remove  model  from the column ModelTypeText. 
# If this doesn't work, we use TypeName as a fallback. Note: When there is no variant, 
# this will result in an empty string.

def normalize_variant(row):
    model = row["ModelText"].strip()
    model_variant = row["ModelTypeText"].strip()
    if model_variant[:len(model)].lower() == model.lower():
        return model_variant[len(model):].strip()
    return row["TypeName"]


# ### Normalize column Zip
# Can be done automatically with e.g. pyzipcode.

def normalize_zip(row):
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


# ## Perform normalization of all columns posible to normalize


NORM_FUNCTIONS = {
    "BodyTypeText": normalize_cartype,
    "BodyColorText": normalize_color,
    "Condition": normalize_condition,
    "Variant": normalize_variant,
    "Zip": normalize_zip,

}

def normalize(row):
    for column, funct in NORM_FUNCTIONS.items():
        row[column] = funct(row)
    return row
    

normalized_data = agg_data.apply(normalize, axis=1)
normalized_data.head()


# ## Perform integration

def integrate(row):
    return pd.Series({
        "carType": row["BodyTypeText"],
        "color": row["BodyColorText"],
        "condition": row["ConditionTypeText"],
        "currency": "CHF", # assume all cars are from/to be sold CH
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

formated_data = normalized_data.apply(integrate, axis=1)
formated_data.head(100)


# ## Export Excel files

with pd.ExcelWriter("solution.xlsx") as writer:
    agg_data.to_excel(writer, sheet_name="Pre-processed Data", index=False, na_rep="null")
    normalized_data.to_excel(writer, sheet_name="Normalized Data", index=False, na_rep="null")
    formated_data.to_excel(writer, sheet_name="Integrated Data", index=False, na_rep="null")
