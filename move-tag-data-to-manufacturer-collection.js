const mongoose = require("mongoose");
var mysql = require('mysql');
const fetch = require("node-fetch");
// for new production
async function Connection(mongoose1) {

    try {
       let url = 'mongodb://sc-loadtesting-solution-cosmosdb:aoiPpe4zDFepOMZ0GJKyPhZrREXSQaKUzPrcwWaSoLQOoVNJ07aDQweNP3q5TE0keellXpqJSfz9ACDbtRxtww==@sc-loadtesting-solution-cosmosdb.mongo.cosmos.azure.com:10255/smartcosmos?ssl=true&replicaSet=globaldb&retrywrites=false';
      	 var newProd = mongoose1.createConnection();
        await newProd.openUri(url);
        newProd.on('error', console.error.bind(console, 'connection error:'));
        return newProd;
    } catch (error) {
        console.log(`\n\n\nSomething went wrong while connecting with MongoDB newProdConnection\n\n\n`, error.meggage);

    }
}

// creating mysql connection with new production 
async function newProdMysqlConnection() {
    var conn = mysql.createConnection({
        host: "loadtesting.mysql.database.azure.com",
        user: "bksfipnb",
        password: "A4Yu5BskyaLl3BFaxGJu8Shwf2",
        database: "smartcosmos"     
    });

    conn.connect((err) => {
        if (err) throw err;
        console.log("Connected!");
        
    });
    return conn;
}

// tenant hash map
async function getTenantHashMap(mysqlConn) {
    var TenantIdMap = new Map();
    return new Promise(function (resolve, reject) {
        mysqlConn.query(
            `select id ,name from tenants`,
            function (err, rows) {
                if (err) throw err;
                if (rows.length == 0) {
                    console.log(`Tenant not found for tenant`);
                } else {
                    for (const data of rows) {
                        TenantIdMap.set(data.id, data.name);
                    }
                }
                resolve(TenantIdMap);
            }
        )
    }
    )
}

// batch Insert function
async function batchInsert(connection, data) {
    try {
        console.log("Data Id for Insertion : ",data)
        let checkBatch = await connection.collection('batches').find({ batchId: data.batchId }).toArray();
        if (checkBatch.length == 0) {
            await connection.collection('batches').insertOne(data);
        }
        return true; 
    }
    catch (error) {
        console.log("Error in inserting batch id details :",data.batchId, "Error is : ",error)
    }

}

async function batchDelete(connection, batchId) {
    try {
        let  counter=0;
        let tagIdArray=[];
        let chunkSize=200;
        let offset=0;
        console.log("Batch Id for Deletion : ",batchId)
       while(true)
       {
        let  localCounter=0;
        let checkBatch = await connection.collection('unassignedtagsdatas').find({ batchId: batchId }).sort({ createdAt: 1 }).limit(2000).toArray();
       console.log("Data Found in delete function query =>",checkBatch.length, "With Offset =>", offset);
	if(checkBatch.length==0)
        {
           break;
	
        }
        if (checkBatch.length >0) 
        {
            	
         for (const data of checkBatch) 
            {
                if (tagIdArray.length < chunkSize && checkBatch.length - localCounter > chunkSize)
		    {
                    tagIdArray.push(data.tagId);
                    counter++;
                    localCounter++
                    continue;
                    }
                tagIdArray.push(data.tagId);
		console.log("TagIds Array=>",tagIdArray.length);    
                await connection.collection('unassignedtagsdatas').deleteMany({ tagId: {$in:tagIdArray} });
                counter++;
                localCounter++
               tagIdArray=[];
		await delay(1000);
             console.log("Total Records Founds",checkBatch.length, "Total Deleted Records => ",counter, "For BatchId=> ",batchId);
	    }
            console.log("Total Deleted Records => ",counter , "For batch Id => ",batchId);
            offset=offset+checkBatch.length;
            await delay(1000);
            //await connection.collection('unassignedtagsdatas').deleteMany({ batchId: batchId });
        }
       }
      //  return true;
    }
    catch (error) {
        console.log("Error in deleting batch data for batchId",batchId, "Error is : ",error)
    }

}

// for custum delay
const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}

async function moveData(connection, enablementdata) {
    const { tenantId, tenantName, batchId, tagId } = enablementdata;
    // getting data from unassignedtagsdatas
    let limit = 5000;
    let offset = 0;
    let bulkData = [];
    let counter = 0;
    let chunkSize = 500;
    let chunkSize02 = 200;
    let chunkSize03 = 50;
    let historyReferenceId = '';
    let secureCount=0;
    let standardCount=0;
    // create data for batches collection

    try {

        while (true) {
            
            let localCounter = 0;
            let responseData = await connection.collection('unassignedtagsdatas').find({ "batchId": batchId }).sort({ createdAt: 1 }).skip(offset).limit(limit).toArray();
            console.log("Founded Tags Response Batch ID: ",batchId , "Response Count=>",responseData.length)
            if (responseData.length == 0) {
                // for different date time
                const now = new Date();
                const utcDatetime = now.toISOString().slice(0, 19) + '.000+00:00';
                let batchInsertData =
                {
                    historyReferenceId: historyReferenceId,
                    tenantId: tenantId,
                    tenantName: tenantName,
                    userId: "migratedUserId",
                    userName: "migratedUserId",
                    batchId: batchId,
                    uploadCount: counter,
                    errorCount: 0,
                    fileName: '',
                    fileLink: '',
                    errorReportLink: '',
                    status: 'Active',
                    operationTime: new Date(),
                    createdAt: new Date(utcDatetime),
                    updatedAt: new Date(utcDatetime),
                }
                // calling batch insert function
                await batchInsert(connection, batchInsertData);
                // updating new tags info to dashboard
                // updating dashboard unused tags count 
                let counts = {
                    secure: secureCount,
                    standard: standardCount
                };
                let objComm=
                {
                    tenantId: tenantId, 
                    count: counts,
                }
                
                updateUnusedHeaderCount(objComm);
                // removing data from unassignedtagsdatas
                await batchDelete(connection, batchId)
                break;

            }
            // reading data array
            // for different date time for each chunk
            const now2 = new Date();
            const utcDatetime2 = now2.toISOString().slice(0, 19) + '.000+00:00';
            for (const data of responseData) {

                //check secureCount and standardCount 
                if(data.secureKey!==null)
                {
                    secureCount++;
                }
                else
                {
                    standardCount++;
                }

                let status = 'Inactive';
                let isActivated = false;
                historyReferenceId = '',
                historyReferenceId = data.historyReferenceId
                if (tagId == data.tagId) {
                    status = 'active';
                    isActivated = true;
                    // sending request for updating enablements count at dashboard
                   // sendingEnablementDataToDashBoard(enablementdata);
                }
                let dataObject =
                {
                    tenantId: tenantId,
                    tenantName: tenantName,
                    userId: "migratedUserId",
                    userName: "migratedUserId",
                    manufacturerName: data.src,
                    customerName: data.customerName,
                    fileName: "",
                    batchId: batchId,
                    tagId: data.tagId,
                    tagInfo: "",
                    tagType: "NFC Tags",
                    tagClass: "",
                    hash: "",
                    secureKey: data.secureKey,
                    inlayItemName: "",
                    inlayType: "",
                    vendorName: "",
                    orderId: data.orderId,
                    orderDate: data.orderDate,
                    orderQuantity: data.orderQuantity,
                    orderQuantityUnit: data.orderQuantityUnit,
                    deliveryDate: data.deliveryDate,
                    deliveryItemName: data.deliveryItemName,
                    deliveryQuantity: data.deliveryQuantity,
                    deliveryQuantityUnit: data.deliveryQuantityUnit,
                    historyReferenceId: historyReferenceId,
                    status: status,
                    isActivated: isActivated,
                    lastValidCounter: "000000",
                    operationTime: new Date(utcDatetime2),
                    createdAt: new Date(utcDatetime2),
                    updatedAt: new Date(utcDatetime2),
                }
                // creating data array of 500 records
                if (bulkData.length < chunkSize && responseData.length - localCounter > chunkSize) {
                    bulkData.push(dataObject);
                    counter++;
                    localCounter++
                    continue;
                }
                // if rest data is less the 500
                if (bulkData.length < chunkSize02 && responseData.length - localCounter > chunkSize02) {
                    bulkData.push(dataObject);
                    counter++;
                    localCounter++
                    continue;
                }
                // if rest data is less the 200
                if (bulkData.length < chunkSize03 && responseData.length - localCounter > chunkSize03) {
                    bulkData.push(dataObject);
                    counter++;
                    localCounter++
                    continue;
                }

                bulkData.push(dataObject);
                // insert into new production DB
                await connection.collection('manufacturertags').insertMany(bulkData);
                bulkData = [];
                counter++;
                localCounter++
                console.log(bulkData.length ,"<", chunkSize ,"&&", responseData.length ,"-", localCounter ,">", chunkSize)
                await delay(200);

            }
            offset = offset + responseData.length;
            console.log("Total Inserted Records For Batch Id=> ", batchId, "LocalCounter===>", localCounter, " Total Inserted Records==>", counter, new Date().toJSON());
            await delay(1000);
        }
    }
    catch (error) {
        console.log("Error in moveData function", error)
    }
}

async function readTags(connection,mysqlConn)
{   
    let limit = 2000;
    let offset = 0;
    localCounter=0;
    try
    {
        let tenantHasMap=await getTenantHashMap(mysqlConn);
        while(true)
        {
            let responseData = await connection.collection('digitizedtags').find({diId:'E0040100A7C9E339'}).sort({ createdAt: 1 }).allowDiskUse(true).skip(offset).limit(limit).toArray();
            if(responseData.length==0)
            {
                break;
            }
            for (const data of responseData) {
                
                // checking data in manufacturee collection first
                let manuData = await connection.collection('manufacturertags').find({tagId:data.diId, status:'Inactive'}).toArray();
                if(manuData.length>0)
                {
                    // calling function for updating manufacturer tags data
                    let UpdateDataObj=
                    {
                        tenantId:data.tenantId,
                        batchId:manuData[0].batchId,
                        tenantName:tenantHasMap.get(data.tenantId),
                        tagId:data.diId,
                        siteId: data.siteId,
                        processId: data.processId,
                        upc: data.productUPC,
                        status: data.status,
                        type: 'secure',
                        beforeStatus: '',
                        operationTime: data.operationTime
                    }
                    updateManufacturerTagsData(connection,UpdateDataObj);
                    continue;
                }
                // checking tagId in unassignedtagsdatas
                let unassignedtagsdatas = await connection.collection('unassignedtagsdatas').find({tagId:data.diId}).toArray();
                console.log("Founded Tags Response Tag ID: ",data.diId , "Response Count=>",unassignedtagsdatas.length)
                if(unassignedtagsdatas.length>0)
                {
                    let moveDataObj=
                    {
                        tenantId:data.tenantId,
                        batchId:unassignedtagsdatas[0].batchId,
                        tenantName:tenantHasMap.get(data.tenantId),
                        tagId:data.diId,
                        siteId: data.siteId,
                        processId: data.processId,
                        upc: data.productUPC,
                        status: data.status,
                        type: 'secure',
                        beforeStatus: '',
                        operationTime: data.operationTime
                    }
                    await moveData(connection, moveDataObj)
                    await delay(1000);
                }
                localCounter++
            }   
            await delay(1000);
            offset=offset+responseData.length;
            console.log("Total Readed Records From DI=> ", localCounter);
        }

    }
    catch(error)
    {
        console.log("Error in reading batches data",error)
    }

}

// updating existing manufacturer tags data
async function updateManufacturerTagsData(connection, data)
{
            try {      
                    let update = {}
                    const filter = { tagId: data.tagId };
                    update.status = 'active'
                    update.isActivated =true
                    console.log("Updating Exiting Tag ID Data =>",filter, update)
                    connection.collection("manufacturertags").findOneAndUpdate(filter, { $set: update });
                    // sending request for updating enablements count at dashboard
                    //sendingEnablementDataToDashBoard(data);
                }
                catch(error)
                {
                console.log("Error in updating existing tag data => Tags ID ",data, "Error is ", error)
                }
}


// updating status of all moved tags from digitized collection

async function updatingStatus(connection)
{   
    let limit = 2000;
    let offset = 0;
    localCounter=0;
    try
    {
        while(true)
        {
            let responseData = await connection.collection('manufacturertags').find({}).sort({ createdAt: 1 }).allowDiskUse(true).skip(offset).limit(limit).toArray();
            if(responseData.length==0)
            {
                break;
            }
            for (const data of responseData) {
                // checking tagId in unassignedtagsdatas
                 
                let digitizedResponse = await connection.collection('digitizedtags').find({diId:data.tagId, status:'enabled'}).toArray();
                console.log("Tags Found in Digitized Collection: ",data.tagId , "Response Count=>",digitizedResponse.length)
                if(digitizedResponse.length>0)
                {
                    let update= {}
                    const filter = { tagId: data.tagId };
                    update.isActivated= true 
                    update.status= 'Active';
                    connection.collection("manufacturertags").findOneAndUpdate(filter, {$set:update});
                }
                localCounter++
            }
            await delay(1000);
            offset=offset+responseData.length;
            console.log("Total Readed Records From DI=> ", localCounter);
        }

    }
    catch(error)
    {
        console.log("Error in reading batches data",error)
    }

}

// updating tags info for Enablements counts in header 
async function sendingEnablementDataToDashBoard(data) {
    try{
    await fetch('https://load.lifecycles.io/api/v1/report/enablement', {
        method: 'POST',
        headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'service-token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.wSYzQkeEsKSbAAVJtVMjodiKPtwOiM6UjulQWyz9qNE'
        },
        body: JSON.stringify({
            tenantId: data.tenantId,
            siteId: data.siteId,
            processId: data.processId,
            upc: data.productUPC,
            status: data.status,
            type: 'secure',
            beforeStatus: '',
            operationTime: data.operationTime
        })
    }).then(response => response.json());
    }
    catch(error)
    {
        console.log("Error in updating sending Enablement Data To DashBoard ",error)
    }
}
// updating tags info for header counts 
async function updateUnusedHeaderCount(data)
{
    try
    {    
        await fetch('https://load.lifecycles.io/api/v1/report/tag', {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'service-token': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.wSYzQkeEsKSbAAVJtVMjodiKPtwOiM6UjulQWyz9qNE'
            },
            body: JSON.stringify({
                tenantId: data.tenantId,
                counts: data.count,
            })
        }).then(response => response.json());
    }
    catch(error)
    {
        console.log("Error in updating updateUnusedHeaderCount ",error)
    }
}

async function main()
{
    console.log("In main function")
    // creating mongos connection
    console.log("Creating mongos connection")
    let connection=await Connection(mongoose);
    // creating mongos connection
    console.log("Creating mysql connection")
    let mysqlConn=await newProdMysqlConnection(mysql);
    
    console.log("calling read tags")
    await readTags(connection,mysqlConn);
    // updating status
    console.log("Calling status update function")
    await updatingStatus(connection);
}

// calling main 
main();
