# test_healthcare_loader.py
"""
Pytest suite for 'healthcare_mongo_loader_optimized.py' (v2)
-----------------------------------------------------------

• 100 % isolated – uses Testcontainers
• Validates: cleaning, typing, dedup, children-inserts, indexes
"""

from pathlib import Path
from datetime import datetime

import pandas as pd
import pytest
from loguru import logger
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

# ─── CUT under test ──────────────────────────────────────────────────────────
# Make sure the file being tested is named 'healthcare_mongo_loader_optimized.py'
# and is in the 'app' directory, or adjust the import path accordingly.
from app import healthcare_mongo_loader_optimized as CUT
# ─────────────────────────────────────────────────────────────────────────────


# ───────────────────── fixtures ──────────────────────────────────────────────
@pytest.fixture(scope="session")
def mongo_client():
    """A real MongoDB instance running in a Docker container."""
    with MongoDbContainer("mongo:7.0.7") as container:
        yield MongoClient(container.get_connection_url())


@pytest.fixture(scope="session")
def db_name():
    return "UnitTestDB"


@pytest.fixture
def collections(mongo_client, db_name):
    """
    Provides a tuple of all collections and ensures they are empty
    before each test runs, guaranteeing test isolation.
    """
    colls = (
        CUT.get_collection(mongo_client, db_name, "Patients"),
        CUT.get_collection(mongo_client, db_name, "Admissions"),
        CUT.get_collection(mongo_client, db_name, "MedicalRecords"),
        CUT.get_collection(mongo_client, db_name, "Billing"),
    )
    # Clean collections before the test to ensure a clean slate
    for coll in colls:
        coll.delete_many({})
    yield colls


@pytest.fixture
def csv_sample(tmp_path: Path) -> Path:
    """A 2-row valid CSV."""
    df = pd.DataFrame(
        {
            "Name": ["John Doe", "Jane Smith"],
            "Age": [35, 42],
            "Gender": ["Male", "Female"],
            "Blood Type": ["A+", "B-"],
            "Medical Condition": ["Diabetes", "Hypertension"],
            "Date of Admission": ["2023-01-15", "2023-02-20"],
            "Doctor": ["Dr. Brown", "Dr. Taylor"],
            "Hospital": ["City General", "Metro Hospital"],
            "Insurance Provider": ["HealthPlus", "MediCare"],
            "Billing Amount": [2500.75, 3200.50],
            "Room Number": [305, 412],
            "Admission Type": ["Urgent", "Elective"],
            "Discharge Date": ["2023-01-22", "2023-03-01"],
            "Medication": ["Insulin", "Lisinopril"],
            "Test Results": ["Normal", "Abnormal"],
        }
    )
    p = tmp_path / "patients.csv"
    df.to_csv(p, index=False)
    return p


# ───────────────────── tests ─────────────────────────────────────────────────
def test_schema_and_index_creation(collections):
    patients, admissions, medical, billing = collections

    for coll, schema in (
        (patients, CUT.patient_schema),
        (admissions, CUT.admission_schema),
        (medical, CUT.medical_record_schema),
        (billing, CUT.billing_schema),
    ):
        CUT.create_schema(coll, schema)

    CUT.create_indexes(patients, admissions, medical, billing)

    # Index existence
    idx_names = patients.index_information()
    assert any(v.get("unique") for v in idx_names.values()), "Unique index absent on Patients"


def test_full_load_happy_path(collections, csv_sample):
    patients, admissions, medical, billing = collections

    # Apply schemas
    for coll, schema in (
        (patients, CUT.patient_schema),
        (admissions, CUT.admission_schema),
        (medical, CUT.medical_record_schema),
        (billing, CUT.billing_schema),
    ):
        CUT.create_schema(coll, schema)

    CUT.create_indexes(patients, admissions, medical, billing)

    CUT.load_data(
        csv_path=str(csv_sample),
        patient_coll=patients,
        admission_coll=admissions,
        medical_coll=medical,
        billing_coll=billing,
        chunk_size=1,  # force several chunks
    )

    # 2 patients, 2 admissions, 2 medicals, 2 billings expected
    assert patients.count_documents({}) == 2
    assert admissions.count_documents({}) == 2
    assert medical.count_documents({}) == 2
    assert billing.count_documents({}) == 2

    jd = patients.find_one({"Name": "John Doe"})
    assert isinstance(jd["Age"], int) and jd["Age"] == 35
    assert jd["Gender"] == "Male"
    assert jd["Blood Type"] == "A+"

    ad = admissions.find_one({"patient_id": jd["_id"]})
    assert ad["Room Number"] == 305
    assert isinstance(ad["Date of Admission"], datetime)


def test_invalid_rows_are_dropped(collections, tmp_path: Path):
    patients, admissions, medical, billing = collections
    
    # CSV with one bad patient (invalid Gender) + one good
    df = pd.DataFrame(
        {
            "Name": ["Bad Guy", "Good Guy"],
            "Age": [20, 30],
            "Gender": ["Alien", "Male"], # "Alien" is invalid
            "Blood Type": ["O+", "O+"],
            "Medical Condition": ["Cancer", "Cancer"],
            "Date of Admission": ["2023-01-01", "2023-01-01"],
            "Discharge Date": ["2023-01-22", "2023-03-01"],
            "Medication": ["Insulin", "Lisinopril"],
            "Room Number": [305, 412],
            "Test Results": ["Normal", "Abnormal"],
            "Billing Amount": [2500.75, 3200.50],
            "Admission Type": ["Urgent", "Elective"],
            "Doctor": ["Dr. Brown", "Dr. Taylor"],
            "Hospital": ["City General", "Metro Hospital"],
            "Insurance Provider": ["HealthPlus", "MediCare"],
        }
    )
    p = tmp_path / "bad.csv"
    df.to_csv(p, index=False)

    for coll, schema in [
        (patients, CUT.patient_schema),
        (admissions, CUT.admission_schema),
        (medical, CUT.medical_record_schema),
        (billing, CUT.billing_schema),
    ]:
        CUT.create_schema(coll, schema)

    CUT.load_data(str(p), patients, admissions, medical, billing, chunk_size=5)

    assert patients.count_documents({}) == 1
    good = patients.find_one()
    assert good["Name"] == "Good Guy"


def test_duplicate_patient_is_upserted(collections, csv_sample):
    patients, admissions, medical, billing = collections

    # Setup & first load
    CUT.create_schema(patients, CUT.patient_schema)
    CUT.create_indexes(patients, admissions, medical, billing)
    CUT.load_data(str(csv_sample), patients, admissions, medical, billing)

    first_count = patients.count_documents({})
    assert first_count == 2

    # Load same file again – upsert must avoid duplicates
    CUT.load_data(str(csv_sample), patients, admissions, medical, billing)

    assert patients.count_documents({}) == first_count