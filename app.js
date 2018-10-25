require('dotenv').config();
const fs = require('fs');
const path = require('path');
const mysql = require('mysql');

let connection = mysql.createConnection({
    host: 'localhost',
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
});

// fn to get our csvs
let getCsvList = (folderPath) => {

    var csvs = fs.readdirSync(folderPath).filter((file) => {
        if (file.indexOf('.csv') > -1) return file;
    });

    return csvs
}

let checkCsvList = (csvList) => {
    return (csvList && csvList.length);
}

// node app.js <folder path> <? columns>

// check if we have enough args
if (process.argv.length < 3) {
    console.error('Not enough arguments. Use node app.js <folder path>');
    process.exit(-1);
}

const folderPath = path.join(process.argv[2]);
const archivePath = path.join(folderPath, 'done');
const errorPath = path.join(folderPath, 'error');

// check if path exists
if (!fs.existsSync(folderPath)) {
    console.error(`Folder path ${folderPath} does not exist`);
    process.exit(-1);
}

// see if there are any new csvs here
let csvsToConsume = getCsvList(folderPath);

if (!checkCsvList(csvsToConsume)) {
    console.log('No .csv files to consume. Exiting.');
    process.exit(0);
}

// if we have new csvs, consume them
csvsToConsume.forEach(file => {

    // get full path
    let fullFilePath = path.join(folderPath, file);

    // if we've got a directory, abort
    if (fs.lstatSync(fullFilePath).isDirectory()) return;

    console.log(`Processing file ${fullFilePath}`);

    // create the query to load the csv into our table
    let sql = `LOAD DATA LOCAL INFILE ? INTO TABLE csvdata FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES (name, email, something) SET createdAt = NOW(), updatedAt = NOW()`;
    let inserts = [fullFilePath];

    sql = mysql.format(sql, inserts);

    // do the query
    connection.query(sql, (err, results, fields) => {

        if (err) {
            console.error(`Error while processing file ${fullFilePath}`);

            // if we error out, create an error path
            if (!fs.existsSync(errorPath)) {
                fs.mkdir(errorPath, (err) => {
                    console.log(err);
                    return;
                });
            }

            let archiveName = file.slice(0, -4).concat('.error.csv');

            fs.rename(fullFilePath, path.join(archivePath, archiveName), (err) => {
                if (err) throw err;

                let csvsRemaining = getCsvList(folderPath);

                if (!checkCsvList(csvsRemaining)) {
                    process.exit(0);
                }

                return;
            });

            return;
        }

        console.log(`Finished processing file ${fullFilePath}`);

        // when we're done, create the archive path
        if (!fs.existsSync(archivePath)) {
            fs.mkdir(archivePath, (err) => {
                if (err) throw err;
                return;
            });
        }

        let archiveName = file.slice(0, -4).concat('.done.csv');

        fs.rename(fullFilePath, path.join(archivePath, archiveName), (err) => {
            if (err) throw err;

            let csvsRemaining = getCsvList(folderPath);

            if (!checkCsvList(csvsRemaining)) {
                process.exit(0);
            }

            return;
        });
    });
});