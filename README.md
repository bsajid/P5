# Migration de données médicales vers MongoDB

## Partie 1 — Migration vers MongoDB (en local)

### Contexte

Le client rencontre des problèmes de scalabilité avec sa gestion de données médicales. Ce projet migre un dataset CSV de patients vers **MongoDB**, une base NoSQL orientée documents qui offre une scalabilité horizontale native.

---

### Concepts clés MongoDB

### Document
L'unité de base de stockage. C'est un objet JSON (stocké en BSON) qui peut contenir des champs imbriqués. Équivalent d'une *ligne* en SQL.

```json
{
  "_id": "ObjectId(...)",
  "personal_info": { "name": "Alice Johnson", "age": 45 },
  "medical_info":  { "condition": "Cancer", "test_results": "Inconclusive" }
}
```

### Collection
Regroupement de documents, analogue à une *table* SQL. Dans ce projet : collection **patients**.

### Base de données
Regroupe un ensemble de collections. Dans ce projet : base **medical_db**.

---

### Schéma de la base de données

Chaque ligne du CSV devient un document imbriqué par domaine métier. Ce schéma permet de lire le dossier complet d'un patient en **une seule requête**, sans jointure.

```
Document "patient"
│
├── personal_info
│   ├── name          String    "Alice Johnson"
│   ├── age           Int32     45
│   ├── gender        String    "Female"
│   └── blood_type    String    "AB-"
│
├── medical_info
│   ├── condition     String    "Cancer"
│   ├── medication    String    "Paracetamol"
│   └── test_results  String    "Inconclusive"
│
├── hospitalization
│   ├── date_of_admission  Date    ISODate("2024-01-03")
│   ├── discharge_date     Date    ISODate("2024-02-26")
│   ├── doctor             String  "Dr. Matthew Smith"
│   ├── hospital           String  "Sons and Miller"
│   ├── room_number        Int32   328
│   └── admission_type     String  "Urgent"
│
├── billing
│   ├── insurance_provider  String  "Aetna"
│   └── amount              Double  18856.28
│
└── metadata
    ├── imported_at  Date    ISODate("2024-...")
    └── source       String  "CSV migration"
```

---

### Structure du projet

```
P5/
├── data/
│   ├── medical_data.csv          # Dataset source
├── migrate.py                    # Script de migration
├── requirements.txt              # Dépendances Python
├── Dockerfile            		  # Dockerfile
├── docker-compose.yml            # docker-compose
└── README.md                     # Ce fichier
```

---


### Prérequis

- Python 3.11
- MongoDB installé et lancé en local
- Installer les dépendances Python :

```bash
pip install -r requirements.txt
```

### Lancer la migration

```bash
python migrate.py
```

### Ce que fait le script

1. Se connecte à MongoDB en local (`mongodb://localhost:27017/`)
2. Lit le fichier `data/medical_data.csv`
3. Vérifie que les données sont correctes (colonnes présentes, doublons, domaine Gender, domaine Blood Type ...)
4. Nettoie et type (supprimer les espaces colonnes, supprimer les doublons, typage des colonnes)
5. Insère les données dans la collection `patients` de la base `medical_db` sur **MongoDB**
6. Crée des index pour accélérer les recherches
7. Vérifie que tout a bien été inséré (**Démonstration CRUD**)
8. Tests d'intégrité dans MongoDB (Nbr de documents en base, Aucun doublon, les 7 index sont créer ...)

---
### Visualiser les données avec MongoDB Compass

1. Ouvre **MongoDB Compass**
2. Connecte-toi sur mongodb://localhost:27017
3. Navigue vers medical_db → patients
4. Clique sur **Refresh** si la base n'apparaît pas

---

### Dépendances

| Package | Rôle |
|---|---|
| pymongo | Driver officiel MongoDB pour Python |
| pandas | Lecture et nettoyage du CSV |


## Partie 2 — Conteneurisation avec Docker

### C'est quoi ?

Au lieu d'installer Python et MongoDB sur sa machine, on utilise Docker.
Docker va créer deux "boîtes" isolées (conteneurs) qui communiquent entre elles :
- une boîte pour **MongoDB**
- une boîte pour **le script Python**

### Prérequis

- Docker installé
- Docker Compose installé

### Lancer avec Docker

```bash
docker-compose up --build
```

C'est tout ! Docker va automatiquement :
1. Démarrer MongoDB
2. Attendre que MongoDB soit prêt
3. Lancer le script de migration

### Voir les logs de la migration

```bash
docker-compose logs migration
```

### Arrêter les conteneurs

```bash
docker-compose down
```
