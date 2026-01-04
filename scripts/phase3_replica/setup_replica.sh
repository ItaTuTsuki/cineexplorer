# T3.1: Configuration du Replica Set
# 1. Commandes pour lancer les 3 noeuds (à lancer dans 3 terminaux)
mongod --replSet rs0 --port 27017 --dbpath data/mongo/db-1 --bind_ip localhost
mongod --replSet rs0 --port 27018 --dbpath data/mongo/db-2 --bind_ip localhost
mongod --replSet rs0 --port 27019 --dbpath data/mongo/db-3 --bind_ip localhost

# 2. Commande d'initialisation (via mongosh --port 27017)
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "localhost:27017" },
    { _id: 1, host: "localhost:27018" },
    { _id: 2, host: "localhost:27019" }
  ]
})