const mongoose = require("mongoose");
mongoose.connect('mongodb://localhost:27017/smartcosmos_prod');
var db = mongoose.connection
db.on('error', console.error.bind(console, 'connection error:'));


db.once('open', async function () {
    console.log("Connection Successful!");
    let nfcArray = [], insertCount = 0, deleteCount = 0;
    let tenantArray = ['8dd5c1c6-def0-42b5-a450-3630d763fe92'];
    for (const tenantId of tenantArray) {
        let offset = 0;
        let ct = 0
        let response
        console.log("Tenant Id :",tenantId)
        
       
            let res =await db.collection("digitizedtags").find({ tenantId: tenantId }).sort({ createdAt: 1 }).skip(offset).limit(22).toArray();
           
            console.log("Fatched Data Length:",res.length)

            for (let i = 0; i < res.length; i++) {
                console.log("From NFC ID =>", res[i].diId)
                ct++;
                // { $or: [ { quantity: { $lt: 20 } }, { price: 10 } ] }
                response = await db.collection("nexproBiz_rawData").find({ $or: [{ 'authentify.nfc': { $eq: res[i].diId }},{ 'authentify.uhf': { $eq: res[i].diId }},{ 'authentify.barcode': { $eq: res[i].diId }} ] }).limit(1).toArray();
                console.log("NexProBiz data =>",response.length)
                if (response.length>0) {
                
                let update = {}
                const filter = { diId: res[i].diId };
                update.operationTime= new Date(response[0]?.authentify.lastOperationTimestamp),
                update.createdAt=new Date(response[0]?.authentify?.lastOperationTimestamp),
                update.lastUpdatedAt = new Date(response[0]?.authentify?.lastOperationTimestamp),

                db.collection("digitizedtags").findOneAndUpdate(filter, { $set: update });
                insertCount++;
                console.log("Update Count", insertCount)
                await delay(500);
              }
            }

            offset = offset + res.length;

        

    }
}
);

const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}