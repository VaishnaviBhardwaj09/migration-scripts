const mongoose = require("mongoose");
var uuid = require('uuid')
var elasticsearch = require('elasticsearch');
const retry = require('retry');
var mysql = require('mysql');
// for elastic search connection


var client = new elasticsearch.Client({ hosts: [''] });
const retryOperation = retry.operation({
    retries: 3, // Number of times to retry
    factor: 1, // Factor to increase the delay between retries in sec
    minTimeout: 10000, // Minimum delay between retries in milliseconds
});

// Attempt the connection using the retry operation
async function checkElasticConnection(retryOperation) {
    retryOperation.attempt((currentAttempt) => {

        console.log(`Connecting to Elasticsearch. Attempt ${currentAttempt}...`);
        client.ping()
            .then(() => {
                console.log('Connected to Elasticsearch!');
            })
            .catch((err) => {
                console.error(`Failed to connect to Elasticsearch: ${err.message}`);
                if (currentAttempt === 3) {
                    console.log("Error in connection establishing from elastic search");
                    return;
                }
                if (retryOperation.retry(err)) {
                    console.log(`Retrying connection after ${retryOperation._timeouts[retryOperation._timeouts.length - 1]} ms...`);
                } else {

                    console.error(`Failed to connect to Elasticsearch after ${currentAttempt} attempts.`);
                }
            });

    });
}

// mongoes connection 
async function newProdConnection(mongoose) {
    try {
        //let url = 'mongodb://solution-mongo:av3BOKETiH4uMxkl9MWlN6aub0vPydYPvYltMi7Kif6ZBgob2VNzE4dxymqFdF7b1sxAJfpzrjfYACDbgPYbuA==@solution-mongo.mongo.cosmos.azure.com:10255/smartcosmos-?ssl=true';
        let url = '';
        var newProd = mongoose.createConnection();
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
        host: "",
        user: "bksfipnb",
        password: "0_AH96D6oduzmFcU",
        database: "smartcosmos"
    });

    conn.connect( (err)=> {
        if (err) throw err;
        console.log("Connected!");
        return conn;
    });
    
}



console.log("Started the Script>>>>>>>>>>>>>>>>>>")


const lowercaseKeys = obj =>
    Object.keys(obj).reduce((acc, key) => {
        acc[key.toLowerCase().trim()] = obj[key];
        return acc;
    }, {});


// c
var dataHashMap = new Map();
var deviceHashMap = new Map();
deviceHashMap.set('c2107b22-b02e-45a7-b126-89b65b054ef6', 'b5b32b82-5da0-41c7-9488-414c8e06a711||Denso Device')
deviceHashMap.set('80a2b0ea-2915-42d1-9f7d-6cc88d6fa269', '1ee5fa16-62ac-4003-9804-11468a07d149||Denso Device')

// adding tenant id Mapping
dataHashMap.set('86904681-eaa5-4b9e-aa49-6b6c4959481c', 'c2107b22-b02e-45a7-b126-89b65b054ef6');
dataHashMap.set('b529be14-2df8-42bd-806d-3a71d5790298', '80a2b0ea-2915-42d1-9f7d-6cc88d6fa269');

dataHashMap.set('67b50ad0-9fc3-4445-a18f-4dbe97f95c22', 'a8c0902c-7795-477e-9788-ef09c731f28e');
dataHashMap.set('9692f4e0-0837-434a-a7df-3eb212c791ea', '22174a41-fa5d-4b83-b861-c915c4c50f6f');
dataHashMap.set('d4878b7b-78a9-4394-8fcd-7ac35a45fa92', 'f6a62be7-fe3a-47e8-b7b1-bc5a8fa1d71a');
dataHashMap.set('96d3bdeb-6465-4a4c-be9c-4c664af4608c', 'ac9bf739-7298-4605-8ea6-d9c5d37152cd');
dataHashMap.set('df2c2366-74c7-4a9c-9d58-5455302496be', '48894078-b2d5-44ce-908d-9af22b3fb9fd');
dataHashMap.set('304ed69d-59b8-4f11-8ae7-36fd6b861597', '01901193-f956-497d-80b5-83b04749a340');
dataHashMap.set('4f9cd098-fc27-4736-b23f-971c3bff75ff', '770e12ca-966f-4a9c-89ed-8caee952160d');
dataHashMap.set('f1c6f5ff-2889-41b8-8efb-8759dadf07cf', 'e963ed12-1237-406b-91e9-befd494f6807');
dataHashMap.set('9a97650e-ae3d-44c7-b401-2130cc5f2574', 'f4754eae-bd02-4c56-a3ec-449dd7b4fb04');
dataHashMap.set('e9102675-dc5f-410a-b84b-d0ace4315ea7', '017b29fb-dd25-4c24-8324-6f653a02c813');
dataHashMap.set('a4e955c3-54a8-4207-b0fd-2fe26d97190b', 'acce80c8-03b6-4131-bc62-2e343650e933');
dataHashMap.set('7b273f6a-7841-4444-99c4-61248940edc6', 'dcfb7a03-cf30-4f9e-a946-3cb69d792f3f');
dataHashMap.set('567c3ad5-b944-4b7e-ad99-70e0e949a9e3', '1b772c96-abe3-4ba8-8368-29e942ea1f8e');
dataHashMap.set('71d3d4bb-3983-4e35-aef7-f8d1aceba7b4', 'be08117c-c7c2-4ede-a0c5-cb06e6f8f531');
dataHashMap.set('eac8868f-6bf8-4769-a64c-31b3f4e3e5d2', '75fbeed2-5248-4986-8bd7-546262a85228');
dataHashMap.set('4b8ec2d5-5154-46f6-9193-dcd576824386', '5e659dbf-86d0-4600-b762-9f0b897c59a8');
dataHashMap.set('296f349f-a485-4904-8728-9c1620e2a92e', '9184689c-8102-4640-8e20-b9a6f6266f38');
dataHashMap.set('12836ef8-a3e3-49fc-a182-95f2ff36b327', 'b44ec091-de8c-450d-bbfd-24e904ba19d1');
dataHashMap.set('39f79e23-b1af-4fff-ae8f-fdeeb7b8a4bb', '7ac654be-3e68-4f97-aeb4-9fac5bb7fcc1');
dataHashMap.set('c82d3042-29df-4d23-a66a-5d55357fbdb7', '328720bb-492c-40af-a1f9-7b544bd81b6d');
dataHashMap.set('9494df17-1786-436e-ac00-3402e1271b78', '07336718-c7f5-42ec-aa5a-d7949f8e9616');
dataHashMap.set('e841072c-6091-4e35-94e7-fc2b6b68b293', '55b9959d-a189-49bd-a468-c8c945d0afc9');
dataHashMap.set('53b4e498-0127-4861-a8c4-4b19dcd5e625', '2d798fbf-d524-41fe-9bea-f87b6342301f');
dataHashMap.set('1fd53384-5053-4940-a533-2c8499db2d0c', '3b0bf357-a5a8-41e5-a899-90a61096d4ae');
dataHashMap.set('1747c453-1d0e-469e-9b97-03f3bf0602fb', 'c242d273-34ac-4459-b313-cf2bee3bbccb');
dataHashMap.set('7830bc05-ac7d-453a-9ee3-965b604f78af', 'e36749a4-f7b9-467e-8d4e-c323e2df9e89');
// user data for authentify
dataHashMap.set('auth0|6115994153f8470069c1287e', '6f3b4d60-760f-42d5-b585-a8bd79e8b3b0||Alissa Bartlett')
dataHashMap.set('23162477-972d-408c-a63b-37dc3c05d682', 'cfddb0d2-1ada-4dc9-b0e3-d3e813996dc4||Aliss_W')
dataHashMap.set('auth0|5eeb84d52f04240013342065', '6b8cf982-a333-4b76-a489-6517e75b0f40||Curtis McConnell')
//dataHashMap.set('', 'beaf2aa7-28a3-4827-8619-a88447e1413c||Dan Bodenheimer')
dataHashMap.set('3cf7ea6f-8245-4f6c-87f2-43ea28b372b8', '47264fb1-f2b4-411a-871f-2568a19aa759||fran')
dataHashMap.set('5dba60e1-b40f-4cdf-a0af-5a35b384c29e', '6e06c1e5-b425-40f4-b6e3-a9cecec804ef||fran')
dataHashMap.set('d629a96d-f13a-42ee-9b33-959312ad5a01', '47a4f4ac-706c-4f5b-ae4a-d75bdb0be81c||fran')
dataHashMap.set('f6bcaa11-c072-4cfb-8439-83a7d45c5302', '518a5fa3-2d57-4ebc-85de-a72dd1ed212e||fran@03')
//dataHashMap.set('', '71f982bd-5bf5-4965-8c87-daba81032069||Itunu')
//dataHashMap.set('', '4b142dc0-a06f-4dea-b91d-bafee57cd9d4||Keno Mullings')
//dataHashMap.set('', '3c775ed4-9bbb-487d-9ca4-8c3857408036||Keri Kilty')
dataHashMap.set('bafa931f-838b-4f3b-aac6-247427879c05', 'b98ff1d2-b557-48d4-a905-fed8bd5888f7||lionel')
dataHashMap.set('auth0|5ee874e4759bb10013397140', 'e6e96a98-a20f-4f6a-a67c-93e333b9038e||Satya Srinivas')
dataHashMap.set('bea1d201-cfb9-4b2b-b75a-cb17f24a346d', 'effb0bf8-850d-451c-8ff5-1a56ac776e91||ssr')
dataHashMap.set('f465c5e0-2c2c-4133-b5ec-cd47c343e1d8', '1f49d004-75c8-431f-8f80-d83e733538bd||ssr1')
dataHashMap.set('', 'e40f4e13-6166-4ea1-ac44-d078d3515e49||Support-Authentify')


// for secretab process data
dataHashMap.set('145dc593-5353-44e4-8926-4a85302ea91f', '33021f99-17d8-4f02-b3c9-2b1c810e64ac')
dataHashMap.set('ef84ef51-5d98-4742-ba79-e786d6038db8', 'b7ff3e62-ade7-4abf-b0d0-766e7cb9d0d0')
dataHashMap.set('969459cd-3780-49a6-87f2-0085a18f5a7b', 'd43a8041-e74c-4d3a-b155-ea2d033af05d')
dataHashMap.set('fbf0c38c-c666-4d4f-be2e-68d8228a1957', '1c7c91f6-c6f6-41a1-ab5f-0a56c2ba2857')
dataHashMap.set('902f12c4-2641-4b71-ac47-46cb252273a1', '314b356b-80ce-4431-96ab-e25ea07ef4f3')
dataHashMap.set('ce7f7629-5067-45be-96cf-670ba7c8137a', '79e087c3-05f8-42a3-b3f5-7501a71ae8bb')
// secret lab user data
dataHashMap.set('0834e877-6182-4220-84e8-b8675edd47d1', '11742570-e483-4349-8a08-8a75f905bfbc||S.XiangDong')
dataHashMap.set('150d18f7-fcb5-4af8-bcb1-2e83363ac28c', '07bf5f5c-07e5-4750-9f96-ccf3725761f7||Secretlab_worker')
dataHashMap.set('1d5b3d3d-b998-4e00-bd9e-53673c69e38f', '44c1c632-31aa-46e0-bddb-7fd79d525db6||D.Feng')
dataHashMap.set('36588a9e-d1e9-4b37-8ee5-f23dd16d11a6', '2de0ad38-3a39-41d4-a0fe-07058c0437d8||S.XiangDong')
dataHashMap.set('3ddddc66-a83a-4925-a286-7578bd0e7574', '8bb2f3a4-7c5e-43c4-95be-d2498c8828c4||S.XiangDong')
dataHashMap.set('4839a841-f1e9-4dc7-ac6c-ac71567ea305', '69560331-f375-4810-9e4d-03658247c635||S.PengFei')
dataHashMap.set('62c1293c-0992-46b7-bb0b-6f9cd965432f', '56e1cd43-b181-4973-938b-4fe69bc1b9bb||S.PengFei')
dataHashMap.set('6c52c300-803c-4e4f-971d-82039d4fe8c9', '38a0bfe1-f0eb-4286-ad74-98e5212645eb||S.XiangDong')
dataHashMap.set('776b5f44-51bd-404f-9de1-fba58e738e38', '9e57a595-e4ca-43a9-bd3f-89965cfc78a8||D.XiaoLong')
dataHashMap.set('8424e47a-bf4f-4d1b-a56c-5b6cb833897c', '9ac7c2c0-0e12-40a0-a225-0706ffd40e4c||J.Chun')
dataHashMap.set('9bb1e904-610e-494b-af90-e31e545807e8', '65c4c605-5a95-4281-b745-35eaa232022f||S.XiangDong')
dataHashMap.set('9fb719ac-e960-4891-8620-b3cdec64843a', 'd441f70b-3c84-4aec-9078-70b128693cac||worker')
dataHashMap.set('a537091b-5073-43b7-9b8d-d5b796016e1d', '9e4d879d-b91b-4cba-83b7-8614ed4ebbba||S.PengFei')
dataHashMap.set('ad2ab1f0-0de4-428f-baf3-6d1fc79eafba', '574bec5e-496d-43bb-9c7d-e65537db51f2||J.Chun')
dataHashMap.set('auth0|5e9d482550f0610cd938cc13', 'e0030f5b-3f6a-4c54-a3cc-ba9914194c7d||Jonathan Seet')
dataHashMap.set('e8e4e846-fe2c-496b-9a0f-337fb5432b9e', 'e1a51cbf-9b47-44b8-9b25-a0649a9807be||S.PengFei')
dataHashMap.set('auth0|5fbe11687861a30076584cee', '4b62c90b-5a68-482e-8054-fcc6d51f895a||Ian Kwan')
dataHashMap.set('ea0e67b5-b8b3-42e4-b066-f4e055417cc4', 'f8b44c0a-0058-4381-ba51-dc27a0311fab||S.PengFei')
dataHashMap.set('fe42ea98-344a-4834-b304-02ccb1a5719e', 'c794ed56-418c-4fbb-87d9-4cea7274de52||S.XiangDong')
dataHashMap.set('c7a3ca10-47b5-4ab1-b670-d1a5c1fe430e', '104a6da4-8b61-4f93-aed3-72f320b6946d||ssr1')
dataHashMap.set('64173743-8a06-4c00-9475-c7e7dd282150', '3e770f90-80b3-4d65-aafd-ad8b1c26659e||S.PengFei')
// secretLab site zone data
dataHashMap.set('9f79d2c0-bd77-4997-8eac-2a189a65bd90', 'eacc85f2-a0eb-4778-a23e-0e9ec9004f25||SECRETLAB SG PTE LTD||77364a60-793d-47b7-b29e-f5530a3e94cc||SECRETLAB SG PTE LTD_DEFAULT');
dataHashMap.set('d20c354a-1566-483e-bb8c-30b0c0553072', '040e6959-61b8-4a39-a848-ea6dad4ba0e5||NINGBO XINLU POLYURETHANE INDUSTRIAL CO., LTD||d167063a-31ef-41cc-adfd-5003fee55889||NINGBO XINLU POLYURETHANE INDUSTRIALCO.LTD_DEFAULT');
dataHashMap.set('bf596b03-28d6-4003-8fb3-dd3e5cd1ee18', 'fc007db9-c7a8-4815-8184-bbb0a14dd20d||SECRETLAB_SATYA TEST ENV||7af403a7-cb35-443f-bcf6-5eeb6af5a6d5||SECRETLAB_SATYA TEST ENV_DEFAULT');
dataHashMap.set('e37ce7c3-0cac-46d5-9695-5529104d0242', 'f8ba00f3-61ad-4290-b5b3-eccd49b3e40f||JIN SHEU ENTERPRISE CO., LTD.||51e005d1-f2da-45eb-b7e3-170e214ac66b||JIN SHEU ENTERPRISE CO., LTD._DEFAULT');
// authentify site zone data
dataHashMap.set('bcd4454b-3612-4ec0-b862-3bd3a13f69e3', 'b5fd056e-aced-4554-adca-32d53985eae3||AUTHENTIFY CARLSBAD||93e22ab5-8805-4f05-acd8-3655eadecbf3||Assembly Zone');
dataHashMap.set('b73c4c6a-dbba-45c9-8f41-f8513de3b911', 'c89406b3-2f74-4d0f-98fd-3739f693869e||AUTHENTIFY SAN MARCOS LAB||54e97b01-6da8-4039-8394-71c4d47406da||AUTHENTIFY SAN MARCOS LAB_DEFAULT');
dataHashMap.set('1bfd5bf6-f15e-430e-9652-3bd13fa97a73', '07854415-aeab-478c-9edd-7f7e44744fb5||AUTHENTIFY STILLWATER MINNESOTA LAB||8521524f-80f3-4299-8aa5-3b3e95f832e0||AUTHENTIFY STILLWATER MINNESOTA LAB_DEFAULT');
dataHashMap.set('f4cb370e-b258-43ce-a3cb-e1d8039d9ecf', '41010713-8421-4b69-ac4a-6a7821082cb8||SATYA_AUTHENTIFYTESTLAB_KAARST||5896c05b-c807-4f66-b486-639bb2f908db||SATYA_AUTHENTIFYTESTLAB_KAARST_DEFAULT');




async function getProductHashMap(connection, tenantId) {
    var productMap = new Map();
    return new Promise(function (resolve, reject) {
        connection.query(
            `select upc,name from products where tenantId='${tenantId}'`,
            function (err, rows) {
                if (err) throw err;
                if (rows.length == 0) {
                    console.log(`Product not found for tenant : ${tenantId}`);
                } else {
                    for (const data of rows) {
                        productMap.set(data.upc, data.id);
                    }
                }
                resolve(productMap);
            }
        )
    }
    )
}






// getting data from elastic search
async function elasticSearchreadData(connection, tenantId, opTime) {
    try {
        const { body } = await connection.search({
            index: 'enablements-gs1',
            type: '_doc',
            from: 0,
            size: 1000,
            body: {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "tenantId": tenantId
                                }
                            },
                            {
                                "range": {
                                    "lastOperationTimestamp": { "gt": opTime }
                                }
                            }
                        ]
                    }
                }
            }
        });

        return body.hits.hits;
    }
    catch (error) {
        console.log("Error in searching data in elastic search for tenantId :", tenantId, "lastOperationTimestamp :", opTime, error)
    }
}

// creating data for mongoDB insertion
async function dataFormator(data, dataHashMap, productHashMap, deviceHashMap) {
    let rawProductData
    let productId
    let productUPC
    let id
    let url
    let type
    let barcode = {}
    let qrcode = {}
    let association = [];
    let encode_ = [];
    let addInput_ = [];
    // getting tenant Id
    let tenantId = dataHashMap.get(data.tenantId);
    if (tenantId === undefined) {
        tenantId = null
    }
    // data trim 
    let dataTrim = data.metadata.product;

    if (dataTrim !== undefined && dataTrim !== null) {
        rawProductData = lowercaseKeys(dataTrim)
    }

    // getting product id from product hashMap    
    if (rawProductData.upc !== undefined) {
        productId = productHashMap.get(rawProductData.upc);
        productUPC = rawProductData.upc
    }
    if (productId === undefined) {
        productId = productHashMap.get(rawProductData?.sku);
        if (productId === undefined) { productId = null; }
        productUPC = rawProductData.sku
    }

    // getting processId from dataHashMap
    let processId = dataHashMap.get(data.processId);
    if (processId === undefined) {
        processId = null
    }
    // getting userid and user Name from dataHashMap
    let userTemp = dataHashMap.get(data.lastOperator.id);
    let userData = userTemp.split('||');
    let userId = userData[0]
    let userName = userData[1]

    // getting device data
    let deviceData_ = deviceHashMap.get(tenantId)
    let deviceData = deviceData_.split('||');
    let status = data.status;

    if (data.metadata.nfc.length > 0) {
        url = data.metadata?.nfc[0].url
        id = data.metadata?.nfc[0].id
        type = "nfc"
    }
    else if (data.metadata.uhf.length > 0) {
        url = data.metadata?.uhf[0].url
        id = data.metadata?.uhf[0].id
        type = "uhf"
    }
    else if (data.metadata.barcode.length > 0) {
        url = data.metadata?.barcode[0].url
        id = data.metadata?.barcode[0].id
        type = "barcode"
    } else {
        url = data.metadata?.qr[0].url
        id = data.metadata?.qr[0].id
        type = "qr"
    }
    // updating association info
    if (data?.metadata?.nfc?.length > 0) {
        for (let i = 0; i < data?.metadata?.nfc?.length; i++) {

            if (data?.metadata?.nfc[i].key !== 'ENCODE_NFC') {
                let nfc_ = {
                    code: data?.metadata?.nfc[i].id,
                    type: data?.metadata?.nfc[i].key,
                    metaInfo: { 'url': data?.metadata?.nfc[i].url }
                }
                association.push(nfc_);
            }

            if (data?.metadata?.nfc[i].key === 'ENCODE_NFC') {
                let encode_nfc = {
                    code: data?.metadata?.nfc[i].id,
                    type: data?.metadata?.nfc[i].key,
                    metaInfo: { 'url': data?.metadata?.nfc[i].url }
                }
                encode_.push(encode_nfc);
            }
        }
    }
    if (data?.metadata?.uhf?.length > 0) {
        for (let i = 0; i < data?.metadata?.uhf.length; i++) {
            if (data?.metadata?.uhf[i].key !== 'ENCODE_UHF') {
                let uhf_ = {
                    code: data?.metadata?.uhf[i].epc,
                    type: data?.metadata?.uhf[i].key,
                    metaInfo: { 'tid': data?.metadata?.uhf[i].tid }
                }
                association.push(uhf_);
            }
            if (data?.metadata?.uhf[i].key === 'ENCODE_UHF') {
                let encode_uhf = {
                    code: data?.metadata?.uhf[i].epc,
                    type: data?.metadata?.uhf[i].key,
                    metaInfo: { 'tid': data?.metadata?.uhf[i].tid }
                }
                encode_.push(encode_uhf);
            }
        }
    }
    if (data?.metadata?.barcode?.length > 0) {
        // for barcode
        for (let i = 0; i < data?.metadata?.barcode.length; i++) {
            if (data?.metadata?.barcode[i].key !== '') {
                let barcode_ = {
                    code: data?.metadata?.barcode[i].code,
                    type: data?.metadata?.barcode[i].key,
                    metaInfo:
                    {
                        subtype: data?.metadata?.barcode[i].subtype,
                        type: data?.metadata?.barcode[i].type,
                    }
                }
                association.push(barcode_);
            }
        }
    }
    if (data?.metadata?.qrcode?.length > 0) {
        for (let i = 0; i < data?.metadata?.qrcode?.length; i++) {
            if (data?.metadata?.qrcode[i].key !== '') {
                let qrcode_ = {
                    code: data?.metadata?.qrcode[i].code,
                    type: data?.metadata?.qrcode[i].key,
                    metaInfo:
                    {
                        subtype: data?.metadata?.qrcode[i].subtype,
                        type: data?.metadata?.qrcode[i].type,
                    }
                }
                association.push(qrcode_);

            }
        }
    }
    if (data?.metadata?.input?.length > 0) {
        for (let i = 0; i < data?.metadata?.input.length; i++) {
            if (data?.metadata?.input[i].key !== '') {
                let input_ = {
                    key: data?.metadata?.input[i].key,
                    value: data?.metadata?.input[i].value,
                }
                addInput_.push(input_);
            }
        }


    }

    let additionalData = {
        association: association,
        addInput: addInput_,
        encode: encode_
    }


    // getting site detail
    let siteData_ = deviceHashMap.get(data.factory.id)
    let siteData = siteData_.split('||');

    let enaleObj = {
        tenantId: tenantId,
        userId: userId,
        userName: userName,
        processId: processId,
        processType: 'Digitization',
        productId: productId,
        productExperienceId: rawProductData?.experienceid,
        productExperienceStudioId: rawProductData?.experiencestudioid,
        productExperienceTenantId: rawProductData?.experiencetenantid,
        productUPC: productUPC,
        status: status.toLowerCase(),
        deviceId: deviceData[0],
        deviceName: deviceData[1],
        siteId: siteData[0],
        siteName: siteData[1],
        zoneId: siteData[2],
        zoneName: siteData[3],
        additionalData,
        diId: id,
        diInfo: data?.metadata,
        primaryURL: url,
        primaryId: id,
        primaryIdType: type,
        productDescription: rawProductData?.description,
        count: '',
        chunkIdentifier: '',
        finalChunk: '',
        fileUrl: '',
        operationTime: new Date(data?.lastOperationTimestamp),
        createdAt: new Date(data?.activationTimestamp),
        lastUpdatedAt: new Date(data?.activationTimestamp),
    }
    return enaleObj;
}


// for headers count

async function sendingDataToDashBoard(data) {
    await fetch('https://portal.lifecycles.io/api/v1/report/enablement', {
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
            type: 'unsecure',
            beforeStatus: '',
            operationTime: data.operationTime
        })
    }).then(response => response.json());
}

const delay = (delayInms) => {
    console.log("DELAY>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    return new Promise(resolve => setTimeout(resolve, delayInms));

}


async function main() {
    // check elastic connection
    await checkElasticConnection(retryOperation);

    // new prod string url mongos connection
    let mongoDb = await newProdConnection(mongoose);

    // for mysql connection
    let mysqlConnection = await newProdMysqlConnection();

    console.log("Script Start >>>>>>>>>>>>>>>>>>>>>>")

    let tenantArray = ['86904681-eaa5-4b9e-aa49-6b6c4959481c', 'b529be14-2df8-42bd-806d-3a71d5790298'];
    for (const tenantId of tenantArray) {

        let productHashMap = await getProductHashMap(mysqlConnection, tenantId);
        let tenantId_ = dataHashMap.get(tenantId);
        try {
            // getting last inserted record for particular tenant

            let lastRecord = await mongoDb.collection("digitizedtags").find({ "tenantId": { $eq: tenantId_ } }).sort({ createdAt: -1 }).limit(1).toArray();

            for (let tenantData of lastRecord) {

                // getting record from elastic search
                let rawData = []
                let lastOpTime = tenantData.createdAt;
                while (true) {
                    // getting old tenant Id

                    rawData = await elasticSearchreadData(client, tenantId, lastOpTime);
                    await delay(200);
                    if (rawData.length === 0) { break; }
                    for (const elasticData of rawData) {

                        let data = elasticData._source
                        // calling data formattor
                        let formatedData = await dataFormator(data, dataHashMap, productHashMap, deviceHashMap);

                        //inserting into digitizedTags collection
                        let checkExistence = await mongoDb.collection("digitizedtags").find({ diId: formatedData.diId, status: formatedData.status }).limit(1).toArray();
                        if (checkExistence.length === 0) {
                            await mongoDb.collection("digitizedtags").insert(formatedData);
                            //calling headers reports
                            sendingDataToDashBoard(formatedData);

                        }
                        await delay(200);
                        lastOpTime = formatedData.createdAt;
                    }
                }
                // to inser in monodb
            }
        }
        catch (err) {
            console.log("Error during reading Tenant Data ", err)
        }
    }
    console.log("End Of Script>>>>>>>>>>>>>>>>>>>>>>")
    // end tenant Id array loop
}

main();



