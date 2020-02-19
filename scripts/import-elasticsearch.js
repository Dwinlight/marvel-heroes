const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'https://elastic:QHeuTPF3kQQ5Xxw0O2HGX3WQ@261265004d634c70a0389c639eaa3108.europe-west2.gcp.elastic-cloud.com:9243' })
const heroesIndexName = 'heroes'

// Création de l'indice

async function run () {


    const body = await client.indices.exists({
        index: heroesIndexName
    })
    console.log(body.statusCode);
    if (body.statusCode == 200) {
        console.log("suppression");
        await client.indices.delete({
            index: "heroes"
        });
    }


    client.indices.create({index: heroesIndexName}, (err, resp) => {
        if(err) console.trace(err.message);
    });

    let heroes = [];
    fs
        .createReadStream('all-heroes.csv')
        .pipe(csv())
        // Pour chaque ligne on créé un document JSON pour l'acteur correspondant
        .on('data', data => {
        heroes.push({
            "id" : data.id,
            "thumbnail": data.imageUrl,
            "name": data.name,
            "aliases": data.aliases.split(','),
            "secretIdentities": data.secretIdentities.split(","),
            "universe": data.universe,
            "partners": data.partners.split(','),
            "description": data.description
        });
})
// A la fin on créé l'ensemble des acteurs dans ElasticSearch
.
    on('end', () => {
        client.bulk(createBulkInsertQuery(heroes), (err, resp) => {
            if(err) console.trace(err.message);
else
    console.log(resp.body.items.length);
    client.close();
})
    ;
})
    ;

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
    function createBulkInsertQuery(heroes) {
        const body = heroes.reduce((acc, heroes) => {
            const {id,thumbnail,name, aliases, secretIdentities, universe, partners, description} = heroes;
        acc.push({index: {_index: heroesIndexName}})
        acc.push({id,thumbnail  ,name, aliases, secretIdentities, universe, partners, description})
        return acc
    },
        []
    )
        ;

        return {body};
    }
}


run().catch(console.error);
