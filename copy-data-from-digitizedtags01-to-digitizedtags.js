const mongoose = require("mongoose");
/// for qa
var db = mongoose.createConnection('mongodb://solution-dev-qamongo:HbylJkfMu8lwRwlAHOWqID9SY256BEnBO9Ulj0awumaJ6VETOOr6cAXu2Od6WQjg5QwOQEzI7ZerACDbyvkF0w==@solution-dev-qamongo.mongo.cosmos.azure.com:10255/smartcosmos_qa?ssl=true&retryWrites=false');
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', async function () {
    console.log("Connection Successful!");
    let offset = 0;
    limit = 200;
    let  insertCounter=0
    while (true) {
        let res = await db.collection("digitizedtags01").find({}).sort({ 'createdAt': -1 }).skip(offset).limit(limit).toArray();
        console.log({ total: res.length });
        if(res.length===0)
        {
            break;
        }
	    let ct = 0
        let bulkData = [];
        console.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Main Loop Start <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        for (let data of res) {
            try {
                ct++;
                let { _id, ...lastData } = data
                 if (bulkData.length < limit && res.length >ct ) { 
                    bulkData.push(lastData);
                   insertCounter++
		    continue;
                }
                bulkData.push(lastData);
               insertCounter++;
		let respIn = await db.collection("digitizedtags").insertMany(bulkData);
                console.log("In Insertion case", ct, new Date())
                bulkData = [];
                await delay(2000); //2 sec
                console.log("Insert => respIn", respIn?.length, "Total Index", res?.length, " Out count index =>", ct)
            }
            catch (error) {
                console.log("Error in ", error);
                await delay(60000); //1 min
            }
        }
        console.log(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Main Loop End <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        offset = offset + res.length;
        console.log("Total copied data from digitizedtags01 to digitizedtags", ct, insertCounter)
    }
}
);
const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));
}

