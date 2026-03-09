// mongo-init.js
// -------------
// Ce script est exécuté automatiquement par MongoDB au premier démarrage.
// Il crée 3 utilisateurs avec 3 niveaux d'accès différents sur medical_db.

// UTILISATEUR 1 : root_admin
// Créé automatiquement par Docker via MONGO_INITDB_ROOT_USERNAME/PASSWORD

// On se place dans la base medical_db
db = db.getSiblingDB("medical_db");

// UTILISATEUR 2 : read_write_user
// Rôle : readWrite + dbAdmin sur medical_db uniquement
// Utilisé par : le script de migration

db.createUser({
  user: "read_write_user",
  pwd:  "ReadWritePassword456!",
  roles: [
    { role: "readWrite", db: "medical_db" },
    { role: "dbAdmin",   db: "medical_db" },
  ]
});
print("Utilisateur 'read_write_user' créé avec succès.");

// UTILISATEUR 3 : read_only_user
// Rôle : read sur medical_db uniquement
// Utilisé par : MongoDB Compass pour consulter les données

db.createUser({
  user: "read_only_user",
  pwd:  "ReadOnlyPassword789!",
  roles: [
    { role: "read", db: "medical_db" },
  ]
});
print("Utilisateur 'read_only_user' créé avec succès.");