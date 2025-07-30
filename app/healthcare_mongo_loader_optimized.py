#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import math
import json
import numbers
import re
import sys
from typing import Any, Dict, NamedTuple

import pandas as pd
from loguru import logger
from pymongo import InsertOne, MongoClient, UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError, OperationFailure # Import OperationFailure
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
        "Age":   {"bsonType": "int", "minimum": 0, "maximum": 125},
        "Gender": {
            "bsonType": "string",
            "enum": ["Male", "Female"]
        },
        "Blood Type": {
            "bsonType": "string",
            "enum": ["A+", "A-", "B+", "B-", "O+", "O-", "AB+", "AB-"]
        },
        "Medical Condition": {
            "bsonType": "string",
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
        "Admission Type": {
            "bsonType": ["string", "null"],
            "enum": ["Urgent", "Emergency", "Elective", None]
        },
        "Room Number": {"bsonType": ["int", "null"], "minimum": 1},
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
        "Test Results": {
            "bsonType": ["string", "null"],
            "enum": ["Normal", "Abnormal", "Inconclusive", None]
        },
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
    TEXT_CLEAN_COLS = (
    "Name", "Doctor", "Hospital", "Insurance Provider",
    "Medication",  # optional
    "Test Results",  # enum but optional
                        )
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
    coll_name = coll.name

    # If collection exists, modify it
    if coll_name in db.list_collection_names():
        logger.info("Collection '%s' exists. Applying schema via collMod...", coll_name)
        db.command({
            "collMod": coll_name,
            "validator": {"$jsonSchema": schema},
            "validationLevel": "strict",
            "validationAction": "error"
        })
    else:
        logger.info("Creating collection '%s' with validator...", coll_name)
        db.create_collection(
            coll_name,
            validator={"$jsonSchema": schema},
            validationAction="error",
            validationLevel="strict"
        )

    # Force re-fetch of options from collection
    applied_options = coll.options()
    validator = applied_options.get("validator", {})

    logger.info("Validator for collection '%s':\n%s", coll_name, json.dumps(validator, indent=2))
    #print(coll.options())
    
    # Defensive: verify validator was applied (especially useful in testcontainers)
    if not validator:
        logger.warning("⚠️  No validator was found in collection '%s' options!", coll_name)
    else:
        logger.success("Schema applied and enforced for collection '%s'", coll_name)



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
    chunk_size: int = 5_000,
) -> None:
    """
    Stream-load the CSV in `chunk_size` batches.

    * Patients               → bulk-UPSERT, **ordered=True** – stop on 1st error, retry remainder one-by-one
    * Admissions / Medicals / Billings → bulk-INSERT, **ordered=True** – same “stop-&-replay” logic

    This way the DB never receives partially-validated documents.
    """
    def _ordered_bulk_safe(coll, ops, label="documents"):
        try:
            coll.bulk_write(ops, ordered=True, bypass_document_validation=False)
            logger.success("Inserted/updated {} {}", len(ops), label)
        except BulkWriteError as bwe:
            # Get the index of the first operation that failed
            first_error = bwe.details["writeErrors"][0]
            idx = first_error["index"]
            
            logger.error("{} bulk interrupted (op {}/{}) - {}",
                         label.capitalize(),
                         idx,
                         len(ops),
                         first_error["errmsg"])

            # Log details for all errors in the batch
            for err in bwe.details.get("writeErrors", []):
                failed_doc = err.get('op')
                logger.error("💥 Failed document:\n{}", failed_doc)
                logger.error("💥 Mongo validation error [op {}]: {}", err.get('index'), err.get('errmsg'))

            # Retry subsequent operations one by one
            logger.info("Retrying remaining {} operations individually...", len(ops) - (idx + 1))
            for op in ops[idx + 1:]:
                try:
                    coll.bulk_write([op], ordered=True, bypass_document_validation=False)
                except BulkWriteError as single_err:
                    # Corrected logging call using {} placeholders
                    logger.error("Retry {} failed – {}", label, single_err)

    logger.info("Loading CSV from %s", csv_path)

    for df in pd.read_csv(csv_path, chunksize=chunk_size):
        # ───── numeric conversions ─────
        df["Date of Admission"] = pd.to_datetime(df["Date of Admission"], errors="coerce")
        df["Discharge Date"]   = pd.to_datetime(df["Discharge Date"],   errors="coerce")
        df["Age"]         = df["Age"].astype(pd.Int64Dtype())
        df["Room Number"] = df["Room Number"].astype(pd.Int64Dtype())
        df["Billing Amount"] = pd.to_numeric(df["Billing Amount"], errors="coerce")

        # ───── text cleaning ─────
        txt_cols = [c for c in C.TEXT_CLEAN_COLS if c in df.columns]
        df[txt_cols] = (
            df[txt_cols]
              .astype(str)
              .apply(lambda s: s.str.strip())
              .replace(NULL_LIKE_STRING_RE, None, regex=True)
        )
        if "Blood Type" in df.columns:
            df["Blood Type"] = df["Blood Type"].str.upper()
        if "Gender" in df.columns:
            df["Gender"] = df["Gender"].str.capitalize()
        title_cols = [
            c for c in (
                "Name", "Doctor", "Hospital", "Insurance Provider",
                "Medication", "Admission Type", "Medical Condition", "Test Results"
            ) if c in df.columns
        ]
        if title_cols:
            df[title_cols] = df[title_cols].apply(lambda s: s.str.title())

        # ───── validate patients ─────
        valid_patients = validate_patients(df)
        if valid_patients.empty:
            logger.warning("No valid patients in this chunk – skipped")
            continue

        # ───── bulk-UPSERT patients (ordered) ─────
        patient_docs = []
        for _, row in valid_patients.iterrows():
            patient_docs.append({
                "Name": row["Name"],
                "Age": int(row["Age"]),  # Force native int
                "Gender": row["Gender"],
                "Blood Type": row["Blood Type"],
                "Medical Condition": row["Medical Condition"]
            })

        patient_upserts = [
            UpdateOne(doc, {"$setOnInsert": doc}, upsert=True)
            for doc in patient_docs
        ]
        _ordered_bulk_safe(patient_coll, patient_upserts, "patients")

        # ───── map key → _id ─────
        key_to_oid: Dict[frozenset, Any] = {}
        for doc in patient_coll.find(
            {"$or": [dict(k) for k in valid_patients["key_tuple"].unique()]},
            {"_id": 1, **{k: 1 for k in C.PATIENT_KEY_FIELDS}},
        ):
            kt = frozenset({k: doc[k] for k in C.PATIENT_KEY_FIELDS}.items())
            key_to_oid[kt] = doc["_id"]
        valid_patients["patient_oid"] = valid_patients["key_tuple"].map(key_to_oid)
        valid_patients = valid_patients[valid_patients["patient_oid"].notna()].copy()
        valid_patients.drop_duplicates(subset=C.PATIENT_KEY_FIELDS, inplace=True)

        # ✅ Drop forbidden internal fields before building docs
        valid_patients = valid_patients.drop(columns=["key_tuple"], errors="ignore")

        # ───── build child document ops ─────
        admissions, medicals, billings = [], [], []
        for _, r in valid_patients.iterrows():
            pid = r["patient_oid"]

            # Admission
            if pd.notna(r["Date of Admission"]) and pd.notna(r["Admission Type"]):
                room = int(r["Room Number"]) if _is_int_like(r["Room Number"]) and r["Room Number"] >= 1 else None
                admissions.append(
                    InsertOne({
                        "patient_id": pid,
                        "Date of Admission": r["Date of Admission"],
                        "Admission Type":  r["Admission Type"],
                        "Room Number":     room,
                        "Discharge Date":  r["Discharge Date"],
                    })
                )

            # Medical record
            if pd.notna(r["Doctor"]) and pd.notna(r["Hospital"]):
                medicals.append(
                    InsertOne({
                        "patient_id": pid,
                        "Doctor":     r["Doctor"],
                        "Hospital":   r["Hospital"],
                        "Medication": r["Medication"] if pd.notna(r["Medication"]) else None,
                        "Test Results": r["Test Results"] if pd.notna(r["Test Results"]) else None,
                    })
                )

            # Billing
            if pd.notna(r["Billing Amount"]) or pd.notna(r["Insurance Provider"]):
                bill_doc = {"patient_id": pid}
                if pd.notna(r["Billing Amount"]):
                    bill_doc["Billing Amount"] = float(r["Billing Amount"])
                if pd.notna(r["Insurance Provider"]):
                    bill_doc["Insurance Provider"] = r["Insurance Provider"]
                billings.append(InsertOne(bill_doc))

        # ───── ordered bulk-inserts for child docs ─────
        _ordered_bulk_safe(admission_coll, admissions, "admissions")
        _ordered_bulk_safe(medical_coll,   medicals,   "medical records")
        _ordered_bulk_safe(billing_coll,   billings,   "billing entries")

    logger.success("Data load complete")

# ──────────────────────── MongoDB User/Role Initialization ──────────────────
def initialize_mongodb_users_and_roles(client: MongoClient, db_name: str):
    """
    Initializes MongoDB roles and users in an idempotent way.
    If a role or user already exists, it logs the information and skips creation.
    """
    logger.info("Connecting to MongoDB as admin for user/role initialization.")
    admin_db = client.admin
    target_db = client[db_name]

    # 1. Create loaderRole
    try:
        admin_db.command({
            "createRole": "loaderRole",
            "privileges": [{
                "resource": { "db": db_name, "collection": "" },
                "actions": ["find","insert", "update", "createIndex", "collMod", "listCollections", "listIndexes"]
            }],
            "roles": []
        })
        logger.success("Role 'loaderRole' created.")
    except OperationFailure as e:
        if "already exists" in str(e):
            logger.info("Role 'loaderRole' already exists. Skipping creation.")
        else:
            raise

    # 2. Create analystRole
    try:
        admin_db.command({
            "createRole": "analystRole",
            "privileges": [{
                "resource": { "db": db_name, "collection": "" },
                "actions": ["find", "listCollections", "listIndexes"]
            }],
            "roles": []
        })
        logger.success("Role 'analystRole' created.")
    except OperationFailure as e:
        if "already exists" in str(e):
            logger.info("Role 'analystRole' already exists. Skipping creation.")
        else:
            raise

    # 3. Create 'loader' user
    try:
        target_db.command({
            "createUser": "loader",
            "pwd": "loaderpwd",
            "roles": [{ "role": "loaderRole", "db": "admin" }]
        })
        logger.success("User 'loader' created.")
    except OperationFailure as e:
        if "already exists" in str(e):
            logger.info("User 'loader' already exists. Skipping creation.")
        else:
            raise

    # 4. Create 'analyst' user
    try:
        target_db.command({
            "createUser": "analyst",
            "pwd": "analystpwd",
            "roles": [{ "role": "analystRole", "db": "admin" }]
        })
        logger.success("User 'analyst' created.")
    except OperationFailure as e:
        if "already exists" in str(e):
            logger.info("User 'analyst' already exists. Skipping creation.")
        else:
            raise

    # 5. Create 'admin' user for the target database
    try:
        target_db.command({
            "createUser": "admin",
            "pwd": "adminpwd",
            "roles": [
                { "role": "dbAdmin", "db": db_name },
                { "role": "userAdmin", "db": db_name }
            ]
        })
        logger.success("User 'admin' created for '%s' database.", db_name)
    except OperationFailure as e:
        if "already exists" in str(e):
            logger.info("User 'admin' for '%s' already exists. Skipping creation.", db_name)
        else:
            raise

    logger.success("MongoDB user and role initialization complete.")


# ───────────────────────────── runner ───────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load healthcare data into MongoDB")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--chunk_size", type=int, default=5000)
    ap.add_argument("--mongo_uri", default="mongodb://localhost:27017",
                    help="MongoDB connection URI (e.g., mongodb://loader:loaderpwd@mongo:27017/HealthcareDB?authSource=HealthcareDB)")
    ap.add_argument("--db_name", default="HealthcareDB")
    ap.add_argument("--admin_mongo_uri", default="mongodb://root:rootpwd@localhost:27017/admin?authSource=admin",
                    help="MongoDB admin connection URI for initial user/role setup") # New argument for admin connection
    args = ap.parse_args()

    client = None
    try:
        # --- Step 1: Initialize users and roles (using admin credentials) ---
        # This part will connect as root, create users/roles, then close the admin connection.
        # It should run only once or be idempotent.
        admin_client_for_init = MongoClient(args.admin_mongo_uri, tz_aware=False) # Create client using the passed URI
        initialize_mongodb_users_and_roles(
            client=admin_client_for_init, # Pass the correctly configured client
            db_name=args.db_name,
            # admin_user="root", # No longer needed by the function signature
            # admin_pwd="rootpwd" # No longer needed by the function signature
        )
        admin_client_for_init.close() # Close the admin connection after initialization

        # --- Step 2: Connect as the 'loader' user for data loading ---
        # Now connect using the loader user, which should now exist
        client = MongoClient(args.mongo_uri, tz_aware=False)
        client.admin.command("ping") # Ping to ensure connection with the loader user
        logger.success("MongoDB connection OK as 'loader' user.")

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
