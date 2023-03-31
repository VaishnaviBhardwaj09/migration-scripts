const mongoose = require("mongoose");

// creating connection from current production
async function currentProdConnection(mongoose2) {

   let url = 'mongodb://sc-loadtesting-solution-cosmosdb:aoiPpe4zDFepOMZ0GJKyPhZrREXSQaKUzPrcwWaSoLQOoVNJ07aDQweNP3q5TE0keellXpqJSfz9ACDbtRxtww==@sc-loadtesting-solution-cosmosdb.mongo.cosmos.azure.com:10255/smartcosmos?ssl=true&replicaSet=globaldb&retrywrites=false';    
    try {
        var oldProd = mongoose2.createConnection();
        await oldProd.openUri(url);
        oldProd.on('error', console.error.bind(console, 'connection error:'));

        return oldProd;
    } catch (error) {
        console.log(`\n\n\nSomething went wrong while connecting with MongoDB currentProdConnection \n\n\n`, error.message);

    }
}

// for custum delay
const delay = (delayInms) => {  
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}


async function readAndUpdate(connection) {
    console.log("Entered in readAndUpdate function");
    // variable declaration
    let limit = 1000;
    let offset = 0;
    let counter=0

    let from = '2020-07-01'
    let to = new Date('2022-03-21');
    try {   //  '42ed8a64-53c1-4b49-91d3-7bc008336180'
        // first one is authentify and another one is secretlab
        let tenantArray = ['c2107b22-b02e-45a7-b126-89b65b054ef6','80a2b0ea-2915-42d1-9f7d-6cc88d6fa269'];
        
        for (const tenantId of tenantArray) {
            console.log("Tenant ID", tenantId);
            while (true) {
                let localCounter = 0;
                let responseData = await connection.collection('digitizedtags').find({ tenantId:tenantId, createdAt: { $gte: new Date(from), $lt: new Date(to) } }).sort({ createdAt: 1 }).skip(offset).limit(limit).toArray();
                console.log("responseData==>",responseData.length)
                if (responseData.length == 0) { break; }
                // reading data
                for (const data of responseData) {

                    const now = new Date();
                    const utcDatetime = now.toISOString().slice(0, 19) + '.000+00:00';
                    let update = {}
                    const filter = { diId: data.diId };
                    update.createdAt = new Date(data.operationTime)
                    update.updatedAt =new Date(utcDatetime)
                    console.log("Updated Data", update)
                    connection.collection("digitizedtags").findOneAndUpdate(filter, { $set: update });
                    localCounter++;
                    counter++;
                    if (localCounter % 100 == 0) {
                        await delay(1000);
                    }
                    
                }
                offset = offset + responseData.length;
                console.log("Currently Running Tenant : ",tenantId, "Current Lenght", responseData.length, "Total Updated Records=>",counter);
            }
        }
    }
    catch (error) {
        console.log("Error in read data from ",error)
    }
}



async function main (mongoose)
{   
    console.log("Entering in main function")
    try{
   // initializing mongos
    let connection=await currentProdConnection(mongoose);
    console.log("After making successfull connection");
    await readAndUpdate(connection);
    }
    catch(error)
    {
        console.log("Error in creating connection in main function",error);
    } 
}

main(mongoose); 
