var mongodb = require("mongodb");
var csv = require("csv-parser");
var fs = require("fs");

var MongoClient = mongodb.MongoClient;
var mongoUrl = "mongodb://localhost:27017";
const dbName = "marvel";
const collectionName = "heroes";

const insertHeroes = (db, callback) => {
    const collection = db.collection('heroes');

    const heroes = [];
    fs.createReadStream('./all-heroes.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
        .on('data', data => {
            heroes.push({

                "id":data.id,
                "name":data.name,
                "description":data.description,
                "imageUrl":data.imageUrl,
                "backgroundImageUrl":data.backgroundImageUrl,
                "externalLink":data.externalLink,
                "secretIdentities":data.secretIdentities,
                "birthPlace": data.birthPlace,
                "occupation":data.occupation,
                "aliases":data.aliases,
                "alignment":data.alignment,
                "firstAppearance":data.firstAppearance,
                "yearAppearance":data.yearAppearance,
                "universe":data.universe,
                "gender":data.gender,
                "race":data.race,
                "type":data.type,
                "height":data.height,
                "weight":data.weight,
                "eyeColor":data.eyeColor,
                "hairColor":data.hairColor,
                "teams":data.teams,
                "powers":data.powers,
                "partners":data.partners,
                "intelligence":data.intelligence,
                "strength":data.strength,
                "speed":data.speechSynthesis,
                "durability":data.durability,
                "power":data.power,
                "combat":data.combat,
                "creators":data.creators
            });
        })
        // A la fin on créé l'ensemble des acteurs dans MongoDB
        .on('end', () => {
            collection.insertMany(heroes, (err, result) => {
                callback(result);
            });
        });
}

MongoClient.connect(mongoUrl, (err, client) => {
    if (err) {
        console.error(err);
        throw err;
    }
    const db = client.db(dbName);
    insertHeroes(db, result => {
        console.log(`${result.insertedCount} actors inserted`);
        client.close();
    });
});
