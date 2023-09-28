//const fetch = require('cross-fetch');
const mongoose = require("mongoose");
const { v4: uuidv4 } = require('uuid');
var mysql = require('mysql');

console.log("Started the Script>>>>>>>>>>>>>>>>>>")

// getting data from server mysql
// host: "sc-database.mysql.database.azure.com",
   
var conn = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "rgbXYZ@9182",
    database: "smartcosmos_prod1"
});

conn.connect(function (err) {
    if (err) throw err;
    console.log("Connected!");
});



async function getProductHashMap(connection, tenantId) {
    var productMap = new Map();
    return new Promise(function (resolve, reject) {
        connection.query(
            `select id ,name from products where tenantId='${tenantId}'`,
            function (err, rows) {
                if (err) throw err;
                if (rows.length == 0) {
                    console.log(`Product not found for tenant : ${tenantId}`);
                } else {
                    for (const data of rows) {
                        productMap.set(data.id, data.name);
                    }
                }
                resolve(productMap);
            }
        )
    }
    )
}

async function getProcessHashMap(connection, tenantId) {
    let processMap = new Map();
    return new Promise(function (resolve, reject) {
        connection.query(
            `select id,name from processes where tenantId='${tenantId}'`,
            function (err, rows) {
                if (rows.length == 0) {
                    console.log(`Process not found for tenant : ${tenantId}`);
                } else {
                    for (const data of rows) {
                        processMap.set(data.id, data.name);
                    }
                }
                console.log(processMap)
                resolve(processMap);
            }

        )
    }
    )
}

mongoose.connect('mongodb://localhost:27017/migration');
var mongoDb = mongoose.connection

//var mongoDb = mongoose.createConnection("mongodb://solution-dev-qamongo:HbylJkfMu8lwRwlAHOWqID9SY256BEnBO9Ulj0awumaJ6VETOOr6cAXu2Od6WQjg5QwOQEzI7ZerACDbyvkF0w==@solution-dev-qamongo.mongo.cosmos.azure.com:10255/smartcosmos_qa?ssl=true&retrywrites=false", { useNewUrlParser: true, useUnifiedTopology: true });
// var mongoDb = mongoose.createConnection('mongodb://sc-production-solution-cosmosdb:Ez36mJ2V6phUbj9kgSyJPQ0ycQuSbLx0wmVUMQZNqRVNbUlywQHPP01qEKEdNQddWHsxPXxiDirpACDbBFp3YA==@sc-production-solution-cosmosdb.mongo.cosmos.azure.com:10255/smartcosmos?ssl=true&replicaSet=globaldb&retrywrites=false');

let enablementsData = {}
let enablementByProcesses = {}
let enablementByProducts = {}

function createdAtDateFormat(date) {
    var newDate=date.slice(0, 19).replace('T', ' ');
    let date2=newDate.split('"');
    return date2[1];
}

function getYYMMDDdate(tempDate) {
    let datte = tempDate.split('T');
    let date2=datte[0].split('"');
    let date = date2[1];
    //console.log("getYYMMDDdate of temDate : ", tempDate, " is ", date)
    return date;
}

function getMonth(tempDate) {
    const mm = tempDate.split("-");
    let month = parseInt(mm[1]);
    return month;
}
async function prepairEnablementsData(datas,processHashMap,productHashMap) {
    for (const data of datas) {
        let date = getYYMMDDdate(JSON.stringify(data.createdAt));
        let month = getMonth(date);
        month=month-1;
        let createdAtDate = createdAtDateFormat(JSON.stringify(data.createdAt));

        if (data.tenantId === null || data.siteId === null || data.processId === null || data.status === null || data.productUPC === null) {
           // console.log("Null Value Condition in prepairEnablementsData ", data.tenantId, data.siteId, data.processId, data.status, data.productUPC)
            continue;
        }
        let processName = processHashMap.get(data.processId);
        if (processName === undefined) {
            console.log("Process not found for :", data.processId)
            continue;
        }
	//let productName = productHashMap.get(data.productId);
        //if (productName === undefined) {
          //  console.log("Produc not found for :", data.productId)
           // continue;
        // }

        let arrayIndex = `${data.tenantId}-${data.siteId}-${date}`
        // insert into array if not exist    
        if (enablementsData[arrayIndex] === undefined) {
            enablementsData[arrayIndex] =
            {
                tenantId: data.tenantId,
                siteId: data.siteId,
                status: data.status,
                type: 'unsecure',
                beforeStatus: '',
                month: month,
                date: date,
                count: 0,
                createdAt: createdAtDate,
                deEnabledCount:0,
                id:data._id
            }

        }
        if(data.status === 'enabled') enablementsData[arrayIndex].count++
        else {
            enablementsData[arrayIndex].deEnabledCount++
            console.log("denabled data:",data._id)
        }
    }
    console.log("enablementsData length ",enablementsData.length);
    //return enablementsData;
}

async function prepairEnablementByProcessData(datas, processHashMap,productHashMap) {
    console.log(datas)
    for (const data of datas) {
        let date = getYYMMDDdate(JSON.stringify(data.createdAt));
        let month = getMonth(date);
        month=month-1;
        let createdAtDate = createdAtDateFormat(JSON.stringify(data.createdAt));
        if (data.tenantId === null || data.siteId === null || data.processId === null || data.status === null || data.productUPC === null) {
           // console.log("Null Value Condition in prepairEnablementsData ", data.tenantId, data.siteId, data.processId, data.status, data.productUPC)
            continue;
        }
        let processName = processHashMap.get(data.processId);
        if (processName === undefined) {
            console.log("Process not found for :", data.processId)
            continue;
        }

//	let productName = productHashMap.get(data.productId);
 //       if (productName === undefined) {
  //          console.log("Produc not found for :", data.productId)
   //         continue;
    //     }

        let arrayIndex = `${data.tenantId}-${data.processId}-${data.siteId}-${date}`
       
        if (enablementByProcesses[arrayIndex] === undefined) {

            // let processData=await getProcessName(data.tenantId, data.processId)
            enablementByProcesses[arrayIndex] =
            {
                tenantId: data.tenantId,
                siteId: data.siteId,
                processId: data.processId,
                processName: processName,
                status: data.status,
                type: 'unsecure',
                beforeStatus: '',
                month: month,
                date: date,
                count: 0,
                createdAt: createdAtDate
            }

        }

        if(data.status === 'enabled') enablementByProcesses[arrayIndex].count++

    }
   console.log("enablementByProcesses length ",enablementByProcesses.length);

}



async function prepairEnablementByProductData(datas, processHashMap, productHashMap) {
    try{
    for (const data of datas) {
        let date = getYYMMDDdate(JSON.stringify(data.createdAt));
        let month = getMonth(date);
        month=month-1;
        let createdAtDate = createdAtDateFormat(JSON.stringify(data.createdAt));
        if (data.tenantId === null || data.siteId === null || data.processId === null || data.status === null || data.productUPC === null) {
           // console.log("Null Value Condition in prepairEnablementsData ", data.tenantId, data.siteId, data.processId, data.status, data.productUPC)
            continue;
        }
        let processName = processHashMap.get(data.processId);
        if (processName === undefined) {
            console.log("Process not found for :", data.processId)
            continue;
        }
        let productName = productHashMap.get(data.productId);
        if (productName === undefined) {
            console.log("Produc not found for :", data.productId)
            continue;
        }
        let arrayIndex = `${data.tenantId}-${data.productId}-${data.siteId}-${date}`
       
        if (enablementByProducts[arrayIndex] === undefined) {
            enablementByProducts[arrayIndex] =
            {
                tenantId: data.tenantId,
                siteId: data.siteId,
                status: data.status,
                upc: data.productUPC,
                productName: productName,
                type: 'unsecure',
                beforeStatus: '',
                month: month,
                date: date,
                count: 1,
                createdAt: createdAtDate
            }
        }
        if(data.status === 'enabled') enablementByProducts[arrayIndex].count++

        // adding data to existing index
    }
   }
    catch(error)
    {
        console.log("Error at creatging data for enablements by products")
    }
    //  return enablementByProducts;
    console.log("enablementByProducts length ",enablementByProducts.length);

}

mongoDb.once('open', async function () {
    //let tenantIdArray = ['b6996074-9aa8-41d7-8a50-32c5f71c6409','ddfbeaf9-40d0-4980-9ef1-a9c72ab44493','cff6c706-9b45-45ff-ba81-614b470bdb38','8a12fdbf-bc44-4722-9695-f390a305d09d','42ed8a64-53c1-4b49-91d3-7bc008336180']
    let tenantIdArray = ['c2107b22-b02e-45a7-b126-89b65b054ef6']
    for (const tenantId of tenantIdArray) {

        let limit = 500
        let count = 0
        let productHashMap = await getProductHashMap(conn, tenantId);
        let processHashMap = await getProcessHashMap(conn, tenantId);
        console.log("productHashMap=>",getProcessHashMap)
        while (true) {
            // console.log("New Count", count)
            let tenantData = await mongoDb.collection('authentify_prod').find({ "tenantId": tenantId , processId:"22174a41-fa5d-4b83-b861-c915c4c50f6f" }).sort({ operationTime: 1 }).skip(count).limit(limit).toArray();
            if (tenantData.length === 0) {
                await insertIntoMysqlDB(conn,enablementsData, enablementByProcesses, enablementByProducts);
                enablementsData = []
                enablementByProcesses = []
                enablementByProducts = []
                break;
            }
            console.log("Tenant Data length", tenantData.length)

            // await prepairEnablementsData(tenantData,processHashMap,productHashMap);
            await prepairEnablementByProcessData(tenantData, processHashMap,productHashMap);
            // await prepairEnablementByProductData(tenantData, processHashMap,productHashMap);
            console.log("Update Data Array For Enablements => ", Object.keys(enablementsData).length)
            console.log("Update Data Array For enablementByProcesses => ", Object.keys(enablementByProcesses).length)
            console.log("Update Data Array For enablementByProducts => ", Object.keys(enablementByProducts).length)

            count = count + tenantData.length
            await delay(1000);
        }

    }// closing tenant array
})



// async function insertIntoMysqlDB(connection,enablementsData, enablementByProcesses, enablementByProducts) {

//     console.log(enablementByProcesses)


//     // console.log("enablements==>",Object.keys(enablementsData).length,"enablementByProcesses=>",Object.keys(enablementByProcesses).length,"enablementByProducts=>",Object.keys(enablementByProducts).length)
//     // // insert into enablements 
//     // //creating  2000 record chunks and bulk insert into database
//     // let insertLimit = 200;
//     // let i = 1;
//     // let sql = 'insert into enablements values ';
//     // let insertCounter=0;
//     // console.log("Enablements to be inserted :",Object.keys(enablementsData).length)
//     // for (const index in enablementsData) {
//     //     let data=enablementsData[index];
        
//     //     let localTeamData=`'`+uuidv4()+`',`+`'`+data.tenantId+`',`+`'`+data.siteId+`',`+`0`+`,`+`0`+`,`+`'`+data.date+`',`+`'`+data.month+`',`+`'`+data.createdAt+`',`+ `'`+data.createdAt+`',`+`null`+`,`+data.count+`,` +data.deEnabledCount;   
//     //     if (i < insertLimit && insertCounter<Object.keys(enablementsData).length-1){
//     //         sql += `(`+ localTeamData+` ), `
//     //         i++;
//     //         insertCounter++;
//     //         continue;
//     //     }
//     //     sql += `(`+ localTeamData +`)`;
//     //     i++;
//     //     insertCounter++;
//     //     connection.query(sql, function (err, result) {
//     //         if (err) {
//     //             console.log("Error in query ", err, sql)

//     //         }
//     //         else
//     //         {
//     //             console.log("Enablements Successfully Inserted :",i, "Total Inserted : ", insertCounter)
//     //         }
//     //     })
//     //     await delay(1000);
//     //     sql = 'insert into enablements values ';
//     //     i = 1;
        
//     // }


//     // // insert into enablementByProcess

//     //  i = 1;
//     //  sql= 'insert into enablementByProcesses values ';
//     // insertCounter=0;
//     // console.log("enablementByProcesses to be inserted :",Object.keys(enablementByProcesses).length)
//     // for (const index in enablementByProcesses) {
//     //     let data=enablementByProcesses[index];
//     //     let localTeamData=`'`+uuidv4()+`',`+`'`+data.tenantId+`',`+`'`+data.siteId+`',`+`'`+data.processId+`',`+`'`+data.processName+`',`+`'`+data.count+`',`+`'`+data.date+`',`+`'`+data.month+`',`+`'`+data.createdAt+`',`+ `'`+data.createdAt+`',`+`null`   
       
//     //     if (i < insertLimit && insertCounter<Object.keys(enablementByProcesses).length-1){
//     //         sql += `( ${localTeamData} ), `
//     //         i++;
//     //         insertCounter++;
//     //         continue;
//     //     }
//     //     sql += `( ${localTeamData} )`;
//     //     i++;
//     //     insertCounter++;
//     //     connection.query(sql, function (err, result) {
//     //         if (err) {
//     //             console.log("Error in query ", err, sql)

//     //         }
//     //         else
//     //         {
//     //             console.log("Enablement By Processes Successfully Inserted :",i, "Total Inserted : ", insertCounter)
//     //         }
//     //     })
//     //     await delay(1000);
//     //      sql = 'insert into enablementByProcesses values ';
//     //     i = 1;

//     // }
//     // // insert into enablementByProducts       
//     // i = 1;
//     // insertCounter=0;
//     // sql = 'insert into enablementByProducts values ';
//     // console.log("enablement By Products to be inserted :",Object.keys(enablementByProducts).length)
//     // for (const index in enablementByProducts) {
//     //     let data=enablementByProducts[index];
//     //     let localTeamData=`'`+uuidv4()+`',`+`'`+data.tenantId+`',`+`'`+data.siteId+`',`+`'`+data.upc+`',`+`'`+data.productName+`',`+`'`+data.count+`',`+`'`+data.date+`',`+`'`+data.month+`',`+`'`+data.createdAt+`',`+ `'`+data.createdAt+`',`+`null`   
       
//     //     if (i < insertLimit && insertCounter<Object.keys(enablementByProducts).length-1){
//     //         sql += `( ${localTeamData} ), `
//     //         i++;
//     //         insertCounter++;
//     //         continue;
//     //     }
//     //     sql += `( ${localTeamData} )`;
//     //     i++;
//     //     insertCounter++;
//     //     connection.query(sql, function (err, result) {
//     //         if (err) {
//     //             console.log("Error in query ", err, sql)

//     //         }
//     //         else
//     //         {
//     //             console.log("Enablement By Products Successfully Inserted :",i, "Total Inserted : ", insertCounter)
//     //         }
//     //     })
//     //     await delay(1000);
//     //     sql= 'insert into enablementByProducts values ';
//     //     i = 1;

//     // }
// }

// async function insertIntoMysqlDB(connection, enablementsData,enablementByProcesses,enablementByProducts) {
//     console.log("enablementByProcesses to be inserted/updated:", Object.keys(enablementByProcesses).length);

//     // Loop through each entry in enablementByProcesses
//     for (const index in enablementByProcesses) {
//         let data = enablementByProcesses[index];

//         // Check if the record exists
//         const checkSql = `SELECT * FROM enablementByProcesses ` +
//             `WHERE tenantId='${data.tenantId}' AND processId='${data.processId}' AND date='${data.date}'`;

//         connection.query(checkSql, function (err, results) {
//             if (err) {
//                 console.log("Error in query ", err, checkSql);
//             } else {
//                 if (results.length > 0) {
//                     // Record exists, update count
//                     const existingCount = results[0].count;
//                     const newCount = existingCount + data.count;

//                     const updateSql = `UPDATE enablementByProcesses ` +
//                         `SET count=${newCount} WHERE tenantId='${data.tenantId}' AND processId='${data.processId}' AND date='${data.date}'`;

//                     connection.query(updateSql, function (updateErr, updateResult) {
//                         if (updateErr) {
//                             console.log("Error in query ", updateErr, updateSql);
//                         } else {
//                             console.log("Enablement By Processes Successfully Updated:", index);
//                         }
//                     });
//                 } else {
//                     // Record doesn't exist, insert new record
//                     const insertSql = `INSERT INTO enablementByProcesses VALUES ` +
//                         `('${uuidv4()}','${data.tenantId}','${data.siteId}','${data.processId}','${data.processName}',` +
//                         `'${data.count}','${data.date}','${data.month}','${data.createdAt}','${data.createdAt}',null)`;

//                     connection.query(insertSql, function (insertErr, insertResult) {
//                         if (insertErr) {
//                             console.log("Error in query ", insertErr, insertSql);
//                         } else {
//                             console.log("Enablement By Processes Successfully Inserted:", index);
//                         }
//                     });
//                 }
//             }
//         });

//         await delay(1000);
//     }

//     // ... (rest of the function)
// }

async function insertIntoMysqlDB(connection, enablementsData, enablementByProcesses, enablementByProducts) {
    console.log(enablementByProcesses);
    
    console.log("enablements==>", Object.keys(enablementsData).length, "enablementByProcesses=>", Object.keys(enablementByProcesses).length, "enablementByProducts=>", Object.keys(enablementByProducts).length);

    let insertLimit = 200;
    let insertCounter = 0;

    // Insert into enablements
    console.log("Enablements to be inserted/updated:", Object.keys(enablementsData).length);
    for (const index in enablementsData) {
        let data = enablementsData[index];

        const checkSql = `SELECT * FROM enablements WHERE tenantId='${data.tenantId}' AND date='${data.date}'`;
        const existingRecords = await queryDatabase(connection, checkSql);
        console.log("existingRecords",existingRecords)

        if (existingRecords.length > 0) {
            const existingCount = existingRecords[0].count || existingRecords[0].unSecureEnabledCount;
            const newCount = existingCount + data.count;

            const updateSql = `UPDATE enablements SET unSecureEnabledCount=${newCount} WHERE tenantId='${data.tenantId}' AND date='${data.date}'`;
            await queryDatabase(connection, updateSql);
            console.log("Enablements Successfully Updated:", index);
        } else {
            const localTeamData = `'${uuidv4()}','${data.tenantId}','${data.siteId}',0,0,'${data.date}','${data.month}','${data.createdAt}','${data.createdAt}',null,${data.count},${data.deEnabledCount}`;
            await insertRecord(connection, 'enablements', localTeamData);
            console.log("Enablements Successfully Inserted:", index);
        }
        await delay(1000);
    }

    // Insert into enablementByProcesses
    console.log("enablementByProcesses to be inserted/updated:", Object.keys(enablementByProcesses).length);
    for (const index in enablementByProcesses) {
        let data = enablementByProcesses[index];

        const checkSql = `SELECT * FROM enablementbyprocesses WHERE tenantId='${data.tenantId}' AND date='${data.date}' AND processId='${data.processId}'`;
        const existingRecords = await queryDatabase(connection, checkSql);

        if (existingRecords.length > 0) {
            const existingCount = existingRecords[0].count;
            const newCount = existingCount + data.count;

            const updateSql = `UPDATE enablementbyprocesses SET count=${newCount} WHERE tenantId='${data.tenantId}' AND date='${data.date}' AND processId='${data.processId}'`;
            await queryDatabase(connection, updateSql);
            console.log("Enablement By Processes Successfully Updated:", index);
        } else {
            const localTeamData = `'${uuidv4()}','${data.tenantId}','${data.siteId}','${data.processId}','${data.processName}','${data.count}','${data.date}','${data.month}','${data.createdAt}','${data.createdAt}',null`;
            await insertRecord(connection, 'enablementbyprocesses', localTeamData);
            console.log("Enablement By Processes Successfully Inserted:", index);
        }
        await delay(1000);
    }

    // Insert into enablementByProducts
    console.log("enablement By Products to be inserted/updated:", Object.keys(enablementByProducts).length);
    for (const index in enablementByProducts) {
        let data = enablementByProducts[index];

        const checkSql = `SELECT * FROM enablementbyproducts WHERE tenantId='${data.tenantId}' AND date='${data.date}' AND upc='${data.upc}'`;
        const existingRecords = await queryDatabase(connection, checkSql);

        if (existingRecords.length > 0) {
            const existingCount = existingRecords[0].count;
            const newCount = existingCount + data.count;

            const updateSql = `UPDATE enablementbyproducts SET count=${newCount} WHERE tenantId='${data.tenantId}' AND date='${data.date}' AND upc='${data.upc}'`;
            await queryDatabase(connection, updateSql);
            console.log("Enablement By Products Successfully Updated:", index);
        } else {
            const localTeamData = `'${uuidv4()}','${data.tenantId}','${data.siteId}','${data.upc}','${data.productName}','${data.count}','${data.date}','${data.month}','${data.createdAt}','${data.createdAt}',null`;
            await insertRecord(connection, 'enablementbyproducts', localTeamData);
            console.log("Enablement By Products Successfully Inserted:", index);
        }
        await delay(1000);
    }
}

async function queryDatabase(connection, sql) {
    return new Promise((resolve, reject) => {
        connection.query(sql, function (err, results) {
            if (err) {
                console.log("Error in query ", err, sql);
                reject(err);
            } else {
                resolve(results);
            }
        });
    });
}

async function insertRecord(connection, table, data) {
    return new Promise((resolve, reject) => {
        const sql = `INSERT INTO ${table} VALUES (${data})`;
        connection.query(sql, function (err, result) {
            if (err) {
                console.log("Error in query ", err, sql);
                reject(err);
            } else {
                resolve(result);
            }
        });
    });
}




const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}

