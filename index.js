//express
// const express = require('express')
// const app = express()
// port = 4000

const connection = require('./mysql')
// const two = app.get('/dispose', (req, res) => {
//         connection.query(`SELECT cdrid FROM task.dispose
//         where cdrid = '26fe698a-c50d-4543-ae-a20c45a46919';`, function (err, result) {
//             if (err) throw err
//             console.log(result)
//             res.send(result)
//          });
//         })

//  a =  () => {
//       connection.query(`SELECT cdrid FROM task.dispose;`, function (err, result) {
//         if (err) throw err
//         console.log(result[1].cdrid)
//      });
//  }

//  a()

const elasticsearch = require('elasticsearch');
const { ELASTIC_SEARCH_URL = 'https://slashAdmin:FlawedByDesign@1612$@elastic-50-uat.slashrtc.in/elastic' } = process.env;
let client = null;


const connect = async () => {
    client = new elasticsearch.Client({
        host: ELASTIC_SEARCH_URL,
        log: { type: 'stdio', levels: [] }
    });
    return client;
};



const ping = async () => {
    let attempts = 0;
    const pinger = ({ resolve, reject }) => {
        attempts += 1;
        client
            .ping({ requestTimeout: 30000 })
            .then(() => {
                console.log('Elasticsearch server available');
                resolve(true);
            })
            .catch(() => {
                if (attempts > 100) reject(new Error('Elasticsearch failed to ping'));
                console.log('Waiting for elasticsearch server...');
                setTimeout(() => {
                    pinger({ resolve, reject });
                }, 1000);
            });
    };

    return new Promise((resolve, reject) => {
        pinger({ resolve, reject });
    });
};

const con = async () => {
    try {
        await connect();
        await ping();
    } catch (error) {
        console.log(error)
    }
}

con()

// let cdrid = "26fe698a-c50d-4543-ae-a20c45a46919"
async function run() {
    const response = await client.search({
        index: 'deliveriesdevlogger2022-11-04',
        body: {
            query: {
                bool: {
                    must: [
                        {
                            match: {
                                "callinfo.agentLegUuid.keyword": "26fe698a-c50d-4543-ae-a20c45a46919",
                            }
                        }
                    ]
                }
            }
        }
    })

    console.log(response.hits.hits)
}



run().catch(console.log)

// connection.query(`SELECT cdrid FROM task.dispose;`, function (err, result) {
//     if (err) throw err
//     var a = result
//     var b = a[1].cdrid
//     console.log(b)
//     if (b === cdrid) {
//       console.log('matched')
//     }
//     else {
//         console.log('not matched')
// }

// for(i=0;i<a.length;i++){
//     if(cdrid === a[i].cdrid){
//         console.log('matched')
//         console.log(i)
//     }
//     else{
//         console.log('not matched')
//     }
// }
// })

const stream = connection.query(
    'SELECT * FROM dispose'
).stream();

stream.on('data', async (e) => {
     updation(e)
    console.log(e);
});

stream.on('end', () => {
    // All rows have been received
    connection.end();
});


const updation = async (e) => {
    const update = {
        script: {
          source: 
                 `ctx._source.callinfo.callTime.talkTime = ${e.agent_talktime_sec}`,
        },
        query: {
          bool: {
            must: {
              term: {
                "callinfo.agentLegUuid.keyword": `${e.cdrid}`,
              },
            },
          },
        },
      };
      client
        .updateByQuery({
          index: "deliveriesdevlogger2022-11-04",
          body: update,
        })
        .then(
          (res) => {
            console.log("Success", res);
          },
          (err) => {
            console.log("Error", err);
          }
        );
}



// app.listen(port, () => {
//     console.log(`app is working on port, ${port}`)
// })


