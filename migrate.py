"""
migrate.py
----------
Migration des données médicales CSV -> MongoDB.
Restructuré en 2 fonctions : lire_csv() + migrer_mongodb()
"""

import os
import pandas as pd
from datetime import datetime
from pymongo import MongoClient

# ==== CONFIGURATION ====
MONGO_URI   = os.getenv("MONGO_URI",        "mongodb://localhost:27017/")
MONGO_USER  = os.getenv("MONGO_USER",       "admin_migration")
MONGO_PASS  = os.getenv("MONGO_PASS",       "motdepasse_secret")
DB_NAME     = os.getenv("MONGO_DB",         "medical_db")
COL_NAME    = os.getenv("MONGO_COLLECTION", "patients")
CSV_PATH    = os.getenv("CSV_PATH",         "data/medical_data.csv")


# ===== FONCTION 1 : Lecture, validation et nettoyage du CSV =====

def lire_csv(chemin_csv):
    """
    Lit le fichier CSV, effectue les tests d'intégrité et le nettoyage.
    Retourne un DataFrame pandas propre et prêt à l'insertion.
    """

    # ---- Lecture ----
    print("\nLecture du CSV...")
    df = pd.read_csv(chemin_csv)
    print(f"{len(df)} lignes lues.")

    # ---- Tests d'intégrité ----
    print("\n--- Tests d'intégrité CSV ---")

    colonnes_attendues = [
        "Name", "Age", "Gender", "Blood Type", "Medical Condition",
        "Date of Admission", "Doctor", "Hospital", "Insurance Provider",
        "Billing Amount", "Room Number", "Admission Type",
        "Discharge Date", "Medication", "Test Results",
    ]
    for col in colonnes_attendues:
        if col not in df.columns:
            print(f"  [ERREUR] Colonne manquante : {col}")
        else:
            print(f"  [OK] Colonne '{col}' présente")

    nb_doublons = df.duplicated(subset=["Name", "Date of Admission"]).sum()
    if nb_doublons > 0:
        print(f"  [AVERT] {nb_doublons} doublon(s) trouvé(s)")
    else:
        print("  [OK] Aucun doublon")

    for col in colonnes_attendues:
        nb_vides = df[col].isnull().sum()
        if nb_vides > 0:
            print(f"  [AVERT] '{col}' : {nb_vides} valeur(s) manquante(s)")

    valeurs_gender_valides    = ["Male", "Female", "Other"]
    valeurs_blood_valides     = ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]
    valeurs_admission_valides = ["Elective", "Emergency", "Urgent"]
    valeurs_test_valides      = ["Normal", "Abnormal", "Inconclusive"]

    for valeur in df["Gender"].unique():
        if valeur not in valeurs_gender_valides:
            print(f"  [AVERT] Gender inconnu : {valeur}")

    for valeur in df["Blood Type"].unique():
        if valeur not in valeurs_blood_valides:
            print(f"  [AVERT] Blood Type inconnu : {valeur}")

    for valeur in df["Admission Type"].unique():
        if valeur not in valeurs_admission_valides:
            print(f"  [AVERT] Admission Type inconnu : {valeur}")

    for valeur in df["Test Results"].unique():
        if valeur not in valeurs_test_valides:
            print(f"  [AVERT] Test Results inconnu : {valeur}")

    for age in df["Age"]:
        if age < 0 or age > 130:
            print(f"  [AVERT] Age hors limites : {age}")
            break
    else:
        print("  [OK] Tous les âges sont entre 0 et 130")

    for montant in df["Billing Amount"]:
        if montant < 0:
            print(f"  [AVERT] Billing Amount négatif : {montant}")
            break
    else:
        print("  [OK] Tous les montants sont positifs")

    df["Date of Admission"] = pd.to_datetime(df["Date of Admission"], dayfirst=True)
    df["Discharge Date"]    = pd.to_datetime(df["Discharge Date"],    dayfirst=True)
    nb_dates_incoh = (df["Discharge Date"] < df["Date of Admission"]).sum()
    if nb_dates_incoh > 0:
        print(f"  [AVERT] {nb_dates_incoh} ligne(s) avec Discharge < Admission")
    else:
        print("  [OK] Les dates sont cohérentes")

    # ---- Nettoyage ----
    print("\n--- Nettoyage ---")

    df.columns = df.columns.str.strip()
    for col in df.select_dtypes("object").columns:
        df[col] = df[col].str.strip()

    nb_avant = len(df)
    df = df.drop_duplicates(subset=["Name", "Date of Admission"])
    if len(df) < nb_avant:
        print(f"  {nb_avant - len(df)} doublon(s) supprimé(s)")

    df["Age"]               = pd.to_numeric(df["Age"],            errors="coerce").astype("Int64")
    df["Room Number"]       = pd.to_numeric(df["Room Number"],    errors="coerce").astype("Int64")
    df["Billing Amount"]    = pd.to_numeric(df["Billing Amount"], errors="coerce")
    df["Date of Admission"] = pd.to_datetime(df["Date of Admission"], dayfirst=True)
    df["Discharge Date"]    = pd.to_datetime(df["Discharge Date"],    dayfirst=True)

    print(f"  {len(df)} lignes après nettoyage")
    return df


# ===== FONCTION 2 : Insertion dans MongoDB + index + CRUD + contrôles =====

def migrer_mongodb(df, uri, user, password, db_name, col_name):
    """
    Se connecte à MongoDB avec authentification, insère les données,
    crée les index, effectue une démo CRUD et des tests d'intégrité.
    """

    # ---- Connexion avec authentification ----
    print("\nConnexion à MongoDB...")
    client = MongoClient(
        uri,
        username=user,
        password=password,
        authSource="medical_db",      # La base où le compte est créé
        authMechanism="SCRAM-SHA-256" # Mécanisme d'authentification sécurisé
    )
    db         = client[db_name]
    collection = db[col_name]
    print("Connecté !")

    # ---- Insertion ----
    print("\n--- Insertion ---")
    nb_existants = collection.count_documents({})
    if nb_existants > 0:
        collection.drop()
        print(f"  Collection vidée ({nb_existants} documents supprimés)")
        collection = db[col_name]

    documents = []
    for _, ligne in df.iterrows():
        doc = {
            "personal_info": {
                "name":       str(ligne["Name"])       if pd.notna(ligne["Name"])  else None,
                "age":        int(ligne["Age"])         if pd.notna(ligne["Age"])   else None,
                "gender":     str(ligne["Gender"]),
                "blood_type": str(ligne["Blood Type"]),
            },
            "medical_info": {
                "condition":    str(ligne["Medical Condition"]),
                "medication":   str(ligne["Medication"]),
                "test_results": str(ligne["Test Results"]),
            },
            "hospitalization": {
                "date_of_admission": ligne["Date of Admission"].to_pydatetime(),
                "discharge_date":    ligne["Discharge Date"].to_pydatetime(),
                "doctor":            str(ligne["Doctor"]),
                "hospital":          str(ligne["Hospital"]),
                "room_number":       int(ligne["Room Number"]) if pd.notna(ligne["Room Number"]) else None,
                "admission_type":    str(ligne["Admission Type"]),
            },
            "billing": {
                "insurance_provider": str(ligne["Insurance Provider"]),
                "amount":             float(ligne["Billing Amount"]) if pd.notna(ligne["Billing Amount"]) else None,
            },
            "metadata": {
                "imported_at": datetime.utcnow(),
                "source":      "CSV migration",
            },
        }
        documents.append(doc)

    collection.insert_many(documents)
    print(f"  {len(documents)} documents insérés")

    # ---- Index ----
    print("\n--- Création des index ---")
    collection.create_index("personal_info.name",                name="idx_name")
    collection.create_index("medical_info.condition",            name="idx_condition")
    collection.create_index("medical_info.test_results",         name="idx_test_results")
    collection.create_index("hospitalization.date_of_admission", name="idx_admission_date")
    collection.create_index("hospitalization.hospital",          name="idx_hospital")
    collection.create_index("hospitalization.admission_type",    name="idx_admission_type")
    collection.create_index("billing.insurance_provider",        name="idx_insurance")
    print("  Index créés")

    # ---- Démonstration CRUD ----
    print("\n--- Démonstration CRUD ---")
    patient_test = {
        "personal_info":   {"name": "Test Patient", "age": 30, "gender": "Male", "blood_type": "O+"},
        "medical_info":    {"condition": "Test", "medication": "Placebo", "test_results": "Normal"},
        "hospitalization": {
            "date_of_admission": datetime(2024, 1, 1),
            "discharge_date":    datetime(2024, 1, 3),
            "doctor": "Dr. Test", "hospital": "Test Hospital",
            "room_number": 0, "admission_type": "Elective",
        },
        "billing":  {"insurance_provider": "None", "amount": 0.0},
        "metadata": {"imported_at": datetime.utcnow(), "source": "demo"},
    }
    res_insert  = collection.insert_one(patient_test)
    print(f"  CREATE : document inséré avec _id = {res_insert.inserted_id}")

    doc_trouve = collection.find_one({"_id": res_insert.inserted_id})
    print(f"  READ   : trouvé → {doc_trouve['personal_info']['name']}")

    nb_diabete = collection.count_documents({"medical_info.condition": "Diabetes"})
    print(f"  READ   : {nb_diabete} patient(s) avec 'Diabetes'")

    collection.update_one({"_id": res_insert.inserted_id}, {"$set": {"personal_info.age": 31}})
    doc_maj = collection.find_one({"_id": res_insert.inserted_id})
    print(f"  UPDATE : âge mis à jour → {doc_maj['personal_info']['age']}")

    collection.delete_one({"_id": res_insert.inserted_id})
    print(f"  DELETE : supprimé → {collection.find_one({'_id': res_insert.inserted_id}) is None}")

    # ---- Tests d'intégrité MongoDB ----
    print("\n--- Tests d'intégrité MongoDB ---")

    nb_docs = collection.count_documents({})
    if nb_docs == len(df):
        print(f"  [OK] {nb_docs} documents en base = {len(df)} lignes CSV")
    else:
        print(f"  [AVERT] {nb_docs} documents en base ≠ {len(df)} lignes CSV")

    pipeline_doublons = [
        {"$group": {"_id": {"name": "$personal_info.name", "date": "$hospitalization.date_of_admission"}, "nb": {"$sum": 1}}},
        {"$match": {"nb": {"$gt": 1}}}
    ]
    if list(collection.aggregate(pipeline_doublons)):
        print(f"  [AVERT] Doublons détectés en base")
    else:
        print("  [OK] Aucun doublon en base")

    exemple = collection.find_one({})
    if exemple:
        print(f"  [OK] age    → {type(exemple['personal_info']['age']).__name__}")
        print(f"  [OK] amount → {type(exemple['billing']['amount']).__name__}")
        print(f"  [OK] date   → {type(exemple['hospitalization']['date_of_admission']).__name__}")

    index_presents = set(collection.index_information().keys())
    index_attendus = ["idx_name", "idx_condition", "idx_test_results",
                      "idx_admission_date", "idx_hospital", "idx_admission_type", "idx_insurance"]
    for idx in index_attendus:
        statut = "[OK]" if idx in index_presents else "[AVERT]"
        print(f"  {statut} Index '{idx}'")

    noms_csv   = set(df["Name"].str.strip().tolist())
    noms_mongo = {d["personal_info"]["name"] for d in collection.find({}, {"personal_info.name": 1, "_id": 0})}
    noms_manquants = noms_csv - noms_mongo
    if noms_manquants:
        print(f"  [AVERT] Noms absents en base : {noms_manquants}")
    else:
        print(f"  [OK] Les {len(noms_csv)} patients du CSV sont tous en base")

    client.close()


# ===== POINT D'ENTRÉE =====
if __name__ == "__main__":
    df_propre = lire_csv(CSV_PATH)
    migrer_mongodb(df_propre, MONGO_URI, MONGO_USER, MONGO_PASS, DB_NAME, COL_NAME)