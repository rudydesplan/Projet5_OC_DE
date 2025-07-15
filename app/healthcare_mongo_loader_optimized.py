#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import math
import numbers
import re
import sys
from typing import Any, Dict, NamedTuple

import pandas as pd
from loguru import logger
from pymongo import InsertOne, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError
from pymongo.write_concern import WriteConcern

# ────────────────────────────── logging ─────────────────────────────
logger.add("mongo_loader.log", rotation="500 KB", retention="5 days", level="INFO")

# ──────────────────────── JSON-Schema definitions ───────────────────
patient_schema = {
    "bsonType": "object",
    "title": "Patient Validation",
    "required": ["Name", "Age", "Gender", "Blood Type", "Medical Condition"],
    "properties": {
        "Name":  {"bsonType": "string"},
        "Age":   {"bsonType": "long", "minimum": 0, "maximum": 125},
        "Gender": {
            "enum": ["Male", "Female"]
        },
        "Blood Type": {
            "enum": ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
        },
        "Medical Condition": {
            "enum": ["Cancer", "Obesity", "Diabetes",
                     "Asthma", "Hypertension", "Arthritis"]
        },
    },
}

admission_schema = {
    "bsonType": "object",
    "title": "Admission Validation",
    "required": ["patient_id", "Date of Admission", "Admission Type"],
    "properties": {
        "patient_id": {"bsonType": "objectId"},
        "Date of Admission": {"bsonType": "date"},
        "Admission Type": {"enum": ["Urgent", "Emergency", "Elective", None]},
        "Room Number": {"bsonType": ["long", "null"], "minimum": 1},
        "Discharge Date": {"bsonType": ["date", "null"]},
    },
}

medical_record_schema = {
    "bsonType": "object",
    "title": "Medical Record Validation",
    "required": ["patient_id", "Doctor", "Hospital"],
    "properties": {
        "patient_id": {"bsonType": "objectId"},
        "Doctor": {"bsonType": "string"},
        "Hospital": {"bsonType": "string"},
        "Medication": {"bsonType": ["string", "null"]},
        "Test Results": {"enum": ["Normal", "Abnormal", "Inconclusive", None]},
    },
}

billing_schema = {
    "bsonType": "object",
    "title": "Billing Validation",
    "required": ["patient_id"],
    "properties": {
        "patient_id": {"bsonType": "objectId"},
        "Billing Amount": {"bsonType": ["double", "null"], "minimum": 0},
        "Insurance Provider": {"bsonType": ["string", "null"]},
    },
}

# ───────────────────────── validation constants ─────────────────────
class C(NamedTuple):
    PATIENT_KEY_FIELDS = tuple(patient_schema["properties"].keys())
    TEXT_CLEAN_COLS = ("Name", "Doctor", "Hospital", "Insurance Provider",
                       "Medication", "Test Results", "Gender", "Blood Type",
                       "Medical Condition", "Admission Type")
    GENDERS = frozenset({"Male", "Female"})
    BLOOD_TYPES = frozenset(
        {"A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"}
    )
    MED_CONDS = frozenset(
        {"Cancer", "Obesity", "Diabetes", "Asthma", "Hypertension", "Arthritis"}
    )

# ────────────────────────── helpers & regexes ───────────────────────
NULL_LIKE_STRING_RE = re.compile(r"(?i)^(?:nan|none|null|n/?a|--|<na>)?$")


def _is_int_like(x) -> bool:
    if pd.isna(x) or isinstance(x, bool):
        return False
    if isinstance(x, numbers.Integral):
        return True
    return isinstance(x, numbers.Real) and math.isfinite(x) and float(x).is_integer()


def build_key_tuple(row: pd.Series) -> frozenset:
    return frozenset(
        {
            "Name": row["Name"],
            "Age": int(row["Age"]),
            "Gender": row["Gender"],
            "Blood Type": row["Blood Type"],
            "Medical Condition": row["Medical Condition"],
        }.items()
    )


# ────────────────── vectorised patient validation ───────────────────
def validate_patients(df: pd.DataFrame) -> pd.DataFrame:
    name_ok = df["Name"].notna()

    age = df["Age"]
    age_ok = (
        age.notna()
        & (age % 1 == 0)
        & age.between(0, 125, inclusive="both")
    )

    gender_ok = df["Gender"].isin(C.GENDERS)
    blood_ok = df["Blood Type"].isin(C.BLOOD_TYPES)
    cond_ok = df["Medical Condition"].isin(C.MED_CONDS)

    valid_mask = name_ok & age_ok & gender_ok & blood_ok & cond_ok
    valid_df = df.loc[valid_mask].copy()

    valid_df["key_tuple"] = valid_df.apply(build_key_tuple, axis=1)

    logger.info(
        "Patient validation: kept {} of {} rows",
        valid_df.shape[0],
        df.shape[0],
    )
    return valid_df


# ───────────────────────── Mongo-utility helpers ────────────────────
def get_collection(client, db, name):
    return client[db].get_collection(name, write_concern=WriteConcern(w=1))


def create_schema(coll, schema):
    db = coll.database
    if coll.name in db.list_collection_names():
        db.command(
            {"collMod": coll.name, "validator": {"$jsonSchema": schema},
             "validationLevel": "strict", "validationAction": "error"}
        )
    else:
        db.create_collection(
            coll.name, validator={"$jsonSchema": schema},
            validationAction="error"
        )
    logger.info("Schema applied for {}", coll.name)


def create_indexes(patients, admissions, medical, billing):
    patients.create_index([(f, 1) for f in C.PATIENT_KEY_FIELDS], unique=True)
    admissions.create_index([("patient_id", 1), ("Date of Admission", 1)])
    medical.create_index([("patient_id", 1), ("Doctor", 1)])
    billing.create_index([("patient_id", 1), ("Billing Amount", 1)])
    logger.info("Indexes created")


# ───────────────────────────── main loader ──────────────────────────
def load_data(
    csv_path: str,
    patient_coll,
    admission_coll,
    medical_coll,
    billing_coll,
    chunk_size: int = 5000,
):
    logger.info("Loading CSV from {}", csv_path)

    for df in pd.read_csv(csv_path, chunksize=chunk_size):
        # ── numeric conversions first ──
        df["Date of Admission"] = pd.to_datetime(
            df["Date of Admission"], errors="coerce"
        )
        df["Discharge Date"] = pd.to_datetime(
            df["Discharge Date"], errors="coerce"
        )
        df["Age"] = pd.to_numeric(df["Age"], errors="coerce")
        df["Room Number"] = pd.to_numeric(df["Room Number"], errors="coerce")
        df["Billing Amount"] = pd.to_numeric(
            df["Billing Amount"], errors="coerce"
        )

        # ── string cleaning ──
        present_text_cols = [c for c in C.TEXT_CLEAN_COLS if c in df.columns]
        df[present_text_cols] = (
            df[present_text_cols]
            .astype(str)
            .apply(lambda s: s.str.strip())
            .replace(NULL_LIKE_STRING_RE, None, regex=True)
        )

        if "Blood Type" in df.columns:
            df["Blood Type"] = df["Blood Type"].str.upper()
        if "Gender" in df.columns:
            df["Gender"] = df["Gender"].str.capitalize()
        title_cols = [
            c
            for c in (
                "Name",
                "Doctor",
                "Hospital",
                "Insurance Provider",
                "Medication",
                "Admission Type",
                "Medical Condition",
                "Test Results",
            )
            if c in df.columns
        ]
        if title_cols:
            df[title_cols] = df[title_cols].apply(lambda s: s.str.title())

        # ── vectorised patient validation ──
        valid_patients = validate_patients(df)
        if valid_patients.empty:
            logger.warning("No valid patients in this chunk — skipping")
            continue

        # Bulk-upsert patients
        key_to_oid: Dict[frozenset, Any] = {}
        upserts = [
            UpdateOne(
                dict(t),
                {"$setOnInsert": dict(t)},
                upsert=True,
            )
            for t in valid_patients["key_tuple"].unique()
        ]
        try:
            if upserts:
                patient_coll.bulk_write(upserts, ordered=False)
        except BulkWriteError as bwe:
            logger.error("Patient upsert errors: {}", bwe.details)

        # Map key_tuple → _id
        cursor = patient_coll.find(
            {"$or": [dict(t) for t in valid_patients["key_tuple"].unique()]},
            {"_id": 1, **{k: 1 for k in C.PATIENT_KEY_FIELDS}},
        )
        for doc in cursor:
            kt = frozenset({k: doc[k] for k in C.PATIENT_KEY_FIELDS}.items())
            key_to_oid[kt] = doc["_id"]

        valid_patients["patient_oid"] = valid_patients["key_tuple"].map(
            key_to_oid
        )

        # ── prepare child docs ──
        admissions, medicals, billings = [], [], []

        for _, r in valid_patients.iterrows():
            pid = r["patient_oid"]

            # Admission (insert if minimal fields are present)
            if pd.notna(r["Date of Admission"]) and pd.notna(r["Admission Type"]):
                room_num = (
                    int(r["Room Number"])
                    if _is_int_like(r["Room Number"]) and r["Room Number"] >= 1
                    else None
                )
                admissions.append(
                    InsertOne(
                        {
                            "patient_id": pid,
                            "Date of Admission": r["Date of Admission"],
                            "Admission Type": r["Admission Type"],
                            "Room Number": room_num,
                            "Discharge Date": r["Discharge Date"],
                        }
                    )
                )

            # Medical record
            if pd.notna(r["Doctor"]) and pd.notna(r["Hospital"]):
                medicals.append(
                    InsertOne(
                        {
                            "patient_id": pid,
                            "Doctor": r["Doctor"],
                            "Hospital": r["Hospital"],
                            "Medication": r["Medication"]
                            if pd.notna(r["Medication"])
                            else None,
                            "Test Results": r["Test Results"]
                            if pd.notna(r["Test Results"])
                            else None,
                        }
                    )
                )

            # Billing
            if pd.notna(r["Billing Amount"]) or pd.notna(r["Insurance Provider"]):
                billing_doc = {"patient_id": pid}
                if pd.notna(r["Billing Amount"]):
                    billing_doc["Billing Amount"] = float(r["Billing Amount"])
                if pd.notna(r["Insurance Provider"]):
                    billing_doc["Insurance Provider"] = r["Insurance Provider"]
                billings.append(InsertOne(billing_doc))

        # ── bulk-insert child docs ──
        for coll, reqs, tag in [
            (admission_coll, admissions, "admissions"),
            (medical_coll, medicals, "medical records"),
            (billing_coll, billings, "billing entries"),
        ]:
            if reqs:
                try:
                    coll.bulk_write(reqs, ordered=False, bypass_document_validation=True)
                    logger.success("Inserted {} {}", len(reqs), tag)
                except PyMongoError as e:
                    logger.error("Failed inserting {}: {}", tag, e)

    logger.success("Data load complete")


# ───────────────────────────── runner ───────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load healthcare data into MongoDB")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--chunk_size", type=int, default=5000)
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017")
    ap.add_argument("--db_name", default="HealthcareDB")
    args = ap.parse_args()

    client = None
    try:
        client = MongoClient(args.mongo_uri, tz_aware=False)
        client.admin.command("ping")
        logger.success("MongoDB connection OK")

        patients = get_collection(client, args.db_name, "Patients")
        admissions = get_collection(client, args.db_name, "Admissions")
        medical = get_collection(client, args.db_name, "MedicalRecords")
        billing = get_collection(client, args.db_name, "Billing")

        for coll, schema in [
            (patients, patient_schema),
            (admissions, admission_schema),
            (medical, medical_record_schema),
            (billing, billing_schema),
        ]:
            create_schema(coll, schema)

        create_indexes(patients, admissions, medical, billing)

        load_data(
            args.csv, patients, admissions, medical, billing, args.chunk_size
        )
        sys.exit(0)
    except Exception as exc:
        logger.exception("Fatal error: {}", exc)
        sys.exit(1)
    finally:
        if client:
            client.close()
            logger.info("MongoDB connection closed")
