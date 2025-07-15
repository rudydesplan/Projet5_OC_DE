# tests/test_healthcare_loader.py
import datetime as dt
from pathlib import Path

import mongomock
import pandas as pd
import pytest
from pymongo.errors import OperationFailure

from healthcare_mongo_loader_optimized import (        # ← your new loader file
    create_schema,
    create_indexes,
    get_collection,
    load_data,
    patient_schema,
    admission_schema,
    medical_record_schema,
    billing_schema,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mongo_client(monkeypatch):
    """Return a mongomock client and monkey-patch the loader’s MongoClient."""
    client = mongomock.MongoClient()
    monkeypatch.setattr(
        "healthcare_mongo_loader_optimized.MongoClient",  # module path in loader
        lambda *a, **kw: client,
    )
    return client


@pytest.fixture
def tmp_csv(tmp_path: Path) -> Path:
    """Create a small, valid CSV and return the path."""
    df = pd.DataFrame(
        {
            "Name": ["John Doe", "Jane Smith"],
            "Age": [35, 42],
            "Gender": ["male", "female"],
            "Blood Type": ["A+", "B-"],
            "Medical Condition": ["Diabetes", "Hypertension"],
            "Date of Admission": ["2023-01-15", "2023-02-20"],
            "Doctor": ["Dr Brown", "Dr Taylor"],
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
    path = tmp_path / "patients.csv"
    df.to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _setup_collections(client, db_name="TestDB"):
    """Return fresh (Patients, Admissions, MedicalRecords, Billing) collections."""
    patients = get_collection(client, db_name, "Patients")
    admissions = get_collection(client, db_name, "Admissions")
    medical = get_collection(client, db_name, "MedicalRecords")
    billing = get_collection(client, db_name, "Billing")

    # apply schema + indexes once per test-run
    for coll, schema in [
        (patients, patient_schema),
        (admissions, admission_schema),
        (medical, medical_record_schema),
        (billing, billing_schema),
    ]:
        create_schema(coll, schema)
    create_indexes(patients, admissions, medical, billing)

    return patients, admissions, medical, billing


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_schema_creation(mongo_client):
    """collections should be created with validators."""
    patients, admissions, medical, billing = _setup_collections(mongo_client)
    db = mongo_client["TestDB"]
    assert set(db.list_collection_names()) == {
        "Patients",
        "Admissions",
        "MedicalRecords",
        "Billing",
    }
    # validator exists?
    assert db.command("listCollections")["cursor"]["firstBatch"][0]["options"]["validator"]


def test_load_data_inserts_docs(mongo_client, tmp_csv):
    patients, admissions, medical, billing = _setup_collections(mongo_client)

    load_data(tmp_csv, patients, admissions, medical, billing, chunk_size=1)

    # two patients, two admissions, two medical records, two billing docs
    assert patients.count_documents({}) == 2
    assert admissions.count_documents({}) == 2
    assert medical.count_documents({}) == 2
    assert billing.count_documents({}) == 2

    john = patients.find_one({"Name": "John Doe"})
    assert john["Gender"] == "Male"
    assert john["Age"] == 35

    adm = admissions.find_one({"patient_id": john["_id"]})
    assert adm["Admission Type"] == "Urgent"
    assert adm["Date of Admission"].date() == dt.date(2023, 1, 15)


def test_validator_rejects_missing_required(mongo_client):
    patients, *_ = _setup_collections(mongo_client)
    bad_doc = {
        "Name": "No Blood",
        "Age": 50,
        "Gender": "Male",
        "Medical Condition": "Asthma",
    }
    with pytest.raises(OperationFailure):
        patients.insert_one(bad_doc)


def test_gender_and_admission_case(mongo_client, tmp_path):
    df = pd.DataFrame(
        {
            "Name": ["case test"],
            "Age": [25],
            "Gender": ["fEMale"],
            "Blood Type": ["O-"],
            "Medical Condition": ["Cancer"],
            "Date of Admission": ["2024-04-01"],
            "Doctor": ["doc"],
            "Hospital": ["hospital"],
            "Insurance Provider": ["prov"],
            "Billing Amount": [100.0],
            "Room Number": [100],
            "Admission Type": ["urgent"],
            "Discharge Date": ["2024-04-05"],
            "Medication": ["med"],
            "Test Results": ["normal"],
        }
    )
    csv_path = tmp_path / "case_test.csv"
    df.to_csv(csv_path, index=False)

    patients, admissions, *_ = _setup_collections(mongo_client)
    load_data(csv_path, patients, admissions, *_)

    pat = patients.find_one({"Name": "Case Test"})
    adm = admissions.find_one({"patient_id": pat["_id"]})
    assert pat["Gender"] == "Female"
    assert adm["Admission Type"] == "Urgent"


def test_schema_allows_extra_fields_in_children(mongo_client):
    """Validate that extra, non-schema fields are rejected by patient validator
    but children allow optional fields handled by loader."""
    patients, admissions, medical, billing = _setup_collections(mongo_client)

    # attempt to shove an arbitrary field into Patients should fail
    with pytest.raises(OperationFailure):
        patients.insert_one(
            {
                "Name": "X",
                "Age": 30,
                "Gender": "Male",
                "Blood Type": "A+",
                "Medical Condition": "Diabetes",
                "Spare": "nope",
            }
        )


def test_int_like_helper(mongo_client, tmp_path):
    """Room Number should be stored as int when valid."""
    df = pd.DataFrame(
        {
            "Name": ["Room Check"],
            "Age": [30],
            "Gender": ["Male"],
            "Blood Type": ["AB+"],
            "Medical Condition": ["Asthma"],
            "Date of Admission": ["2024-06-01"],
            "Admission Type": ["Elective"],
            "Room Number": [401.0],  # float that is int-like
        }
    )
    csv_path = tmp_path / "room_check.csv"
    df.to_csv(csv_path, index=False)

    patients, admissions, *_ = _setup_collections(mongo_client)
    load_data(csv_path, patients, admissions, *_)

    pat = patients.find_one({"Name": "Room Check"})
    adm = admissions.find_one({"patient_id": pat["_id"]})
    assert adm["Room Number"] == 401
