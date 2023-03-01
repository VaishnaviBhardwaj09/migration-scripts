const mongoose = require("mongoose");
mongoose.connect('mongodb://localhost:27017/smartcosmos_prod');
var db = mongoose.connection
db.on('error', console.error.bind(console, 'connection error:'));


db.once('open', async function () {
    console.log("Connection Successful!");
    let nfcArray = [], insertCount = 0, deleteCount = 0;
    let tenantArray = ['f9452900-f90a-40d5-b2a0-a89577774ea2'];
    for (const tenantId of tenantArray) {
        let offset = 0;
        let ct = 0
        let response
        console.log("Tenant Id :",tenantId)
        
        while (true) {
            let res =await db.collection("digitizedtags").find({ tenantId: tenantId }).sort({ createdAt: -1 }).skip(offset).limit(200).toArray();
            if(res.length===0)
            {
                break;
            }
            console.log("Fatched Data Length:",res.length)

            for (let i = 0; i < res.length; i++) {
                console.log("From NFC ID =>", res[i].diId)
                ct++;
                // { $or: [ { quantity: { $lt: 20 } }, { price: 10 } ] }
                response = await db.collection("kilchoman").find({ $or: [{ 'secretLab.nfc': { $eq: res[i].diId }},{ 'secretLab.uhf': { $eq: res[i].diId }} ] }).limit(1).toArray();
                console.log("KilchoMan data =>",response.length)
                if (response.length === 0) {
                    break;
                }

                let update = {}
                const filter = { diId: res[i].diId };
                update.operationTime= new Date(response[0]?.secretLab.lastOperationTimestamp),
                update.createdAt=new Date(response[0]?.secretLab?.activationTimestamp),
                update.lastUpdatedAt = new Date(response[0]?.secretLab?.activationTimestamp),

                db.collection("digitizedtags").findOneAndUpdate(filter, { $set: update });
                insertCount++;
                console.log("Update Count", insertCount)
            }
            await delay(2000);
            offset = offset + res.length;

        }

    }
}
);

const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}