var mysql = require('mysql');

// creating mysql connection
var conn = mysql.createConnection({
    host: "localhost",
    user: "root",
    password: "rgbXYZ@9182",
    database: "smartcosmos_prod"
});

conn.connect(function (err) {
    if (err) throw err;
    console.log("Connected!");
});


async function updateEnablementsMonth(connection, tenantId) {
    return new Promise(function (resolve, reject) {
        connection.query(
            `update enablements set month=(month-1) where tenantId='${tenantId}'`,
            function (err, response) {
                if (err) {
                    console.log(`Data Successfully updated : ${tenantId}`);
                }
                if (response != '') {
                    console.log(`Data Successfully updated : ${tenantId}`);
                }
            }
        )
    }
    )
}

async function updateEnablementByProductMonth(connection, tenantId) {

    return new Promise(function (resolve, reject) {
        connection.query(
            `update enablementByProducts set month=(month-1) where tenantId='${tenantId}'`,
            function (err, response) {
                if (err) {
                    console.log(`Data Successfully updated : ${tenantId}`);
                }
                if (response != '') {
                    console.log(`Data Successfully updated : ${tenantId}`);
                }
            }
        )
    }
    )
}


async function updateEnablementByProcessMonth(connection, tenantId) {

    return new Promise(function (resolve, reject) {
        connection.query(
            `update enablementByProcesses set month=month-1 where tenantId='${tenantId}'`,
            function (err, response) {
                if (err) {
                    console.log(`Data Successfully updated : ${tenantId}`);
                }
                if (response != '') {
                   console.log(`Data Successfully updated : ${tenantId}`);
                }
            }
        )
    }
    )
}

let tenantIdArray = ['c2107b22-b02e-45a7-b126-89b65b054ef6', 'f9452900-f90a-40d5-b2a0-a89577774ea2', '8dd5c1c6-def0-42b5-a450-3630d763fe92', '213ec29c-de8d-465f-8e1f-61d65246726c', '80a2b0ea-2915-42d1-9f7d-6cc88d6fa269'];

async function start(conn) {
    for (const tenantId of tenantIdArray) {
        await updateEnablementsMonth(conn, tenantId);
        await updateEnablementByProductMonth(conn, tenantId);
        await updateEnablementByProcessMonth(conn, tenantId);
    }
    console.log("Data successfully updated");
}



start(conn);