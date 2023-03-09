const mongoose = require("mongoose");

// for old production user
var oldProd = mongoose.createConnection("mongodb://localhost:27017/currentProduction");
// for new production user
var newProd = mongoose.createConnection("mongodb://localhost:27017/smartcosmos_prod");

// creating customer HashMap
async function getCustomerHashMap(connection, batchId) {
    let customerHashMap = new Map();
    let restData = await connection.collection('CustomerBatch').find({ "batchId": batchId }).toArray();
    for (const data of restData) {
        // getting customer details
        let customer = await connection.collection('Customer').find({ "customerId": data.customerId }).toArray();
        let batchObject = {
            customerName: customer[0].customerName,
            deliveryDate: data.deliveryDate,
            deliveryItemName: data.deliveryItemName,
            deliveryQuantity: data.deliveryQuantity,
            deliveryQuantityUnit: data.deliveryQuantityUnit,
            historyReferenceId: data.refOrderItemId,
            orderId: data.orderId,
            orderDate: data.orderDate,
            orderQuantity: data.orderQuantity,
            orderQuantityUnit: data.orderQuantityUnit,
        }
        customerHashMap.set(batchId, batchObject);
       
    }
    return customerHashMap;

}
    
async function readTagData(oldProdConnection, newProdConnection) {

    console.log("Ready To Import Data from Current Production environement to New Production environment")
    let counter = 0
    limit = 5000;
    let chunkSize = 500;
    // getting distinct batch id
    let batchData_ = await oldProdConnection.collection('Tag').distinct("bId");
    console.log("batchData_",batchData_);
    for (let i=0;i<batchData_.length;i++) {
        console.log("New Batch ID :",batchData_[i]);
        let bulkData = [];
        let offset = 0;
        //calling batchId HashMap
        let custumerData_ = await getCustomerHashMap(oldProdConnection, batchData_[i])
        let custumerData=custumerData_.get(batchData_[i])
        console.log(custumerData);
    
        while (true) {
            let localCounter = 0;
            let data_ = await oldProdConnection.collection('Tag').find({ "bId": batchData_[i] }).sort({ ct: 1 }).skip(offset).limit(limit).toArray();
            console.log("Total Record Found For BatchID=>",batchData_[i], "Using Offest ", offset, "Data Found==>",data_.length )
            if(data_.length===0)
            {
                break;
            }
            for (const data of data_) 
            {
                let final_data =
                {
                    batchId:batchData_[i],
                    tagId: data.htId,
                    secureKey: data.skey,
                    src: data.src,
                    customerName: custumerData?.customerName,
                    deliveryDate: custumerData?.deliveryDate,
                    deliveryItemName: custumerData?.deliveryItemName,
                    deliveryQuantity: custumerData?.deliveryQuantity,
                    deliveryQuantityUnit: custumerData?.deliveryQuantityUnit,
                    historyReferenceId: custumerData?.refOrderItemId,
                    orderId: custumerData?.orderId,
                    orderDate: custumerData?.orderDate,
                    orderQuantity: custumerData?.orderQuantity,
                    orderQuantityUnit: custumerData?.orderQuantityUnit,
                }
              
                //  5100
                // 0 < 500 && 5100 - 5000 > 500
                if (bulkData.length < chunkSize && data_.length - localCounter > chunkSize) {
                    bulkData.push(final_data);
                    counter++;
                    localCounter++
                    continue;
                    
                }
               
                bulkData.push(final_data);
                // insert into new production DB
                await newProdConnection.collection('unassignedTagsData').insertMany(bulkData);
                bulkData = [];
                counter++;
                localCounter++
                await delay(1000);
            }
            offset=offset+data_.length;
            console.log("Total Inserted Records For Batch Id=> ",batchData_[i], "LocalCounter===>",localCounter," Total Inserted Records==>",counter);
            await delay(1000);

        }
    }


}


// for custum delay
const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}

// calling main function
readTagData(oldProd, newProd);


