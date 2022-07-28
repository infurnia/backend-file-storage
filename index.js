const utils = require('./utils');
const clean_file_path = utils.clean_file_path;
const InfurniaGCSClient = require('./libs/gcp/gcs');
const multerGoogleStorage = require("@infurnia/multer-cloud-storage");
const { OK_RESPONSE_TEXT } = require('../../controllers/utils');
const crypto = require('crypto');
const path = require('path');

const PROJECT_ID = process.env.GCP_PROJECT_ID; 
if (!PROJECT_ID) { 
    throw new Error(`env var GCP_PROJECT_ID is not set`);
}
const BUCKET_NAME = process.env.GCS_FILE_STORAGE_BUCKET_NAME;
if (!BUCKET_NAME) {
    throw new Error(`env var GCS_FILE_STORAGE_BUCKET_NAME is not set`);
}

const gcs_client = new InfurniaGCSClient(PROJECT_ID, null);

const getMulter = function(
    destination,
    filename_fn = function (req, file, cb) {
        crypto.pseudoRandomBytes(16, function (err, raw) {
            if (err) return cb(err);
            cb(null, raw.toString('hex') + path.extname(file.originalname));
        })
    }
) {
    try{
        return multerGoogleStorage.storageEngine({
            bucket: BUCKET_NAME,
            projectId: PROJECT_ID,
            uniformBucketLevelAccess: true,
            destination,
            filename: filename_fn
        });
    }
    catch(err){
        console.error(`error in file_storage/index.js/getMulter`);
        console.error(err);
        throw err;
    }
}

const listFiles = async (directory) => {
    try{
        let files_list = await gcs_client.list_files_in_bucket(BUCKET_NAME, directory);
        return files_list[0].map(x=> x.name);
    }
    catch(err){
        console.error(`error in file_storage/index.js/listFiles`);
        console.error(err);
        throw err;
    }
}

//Uploads file from a path to the main uploads bucket
const writeFileFromPath = async (sourcePath, destinationPath) => {
    //by default will upload to one bucket only...change priorityLevel to change
    try{
        destinationPath = clean_file_path(destinationPath);
        await gcs_client.write_file_from_path_to_bucket(BUCKET_NAME, destinationPath, sourcePath);
        return OK_RESPONSE_TEXT;
    }   
    catch(err){
        console.error(`error in file_storage/index.js/writeFileFromPath`);
        console.error(err);
        throw err;
    }
}

//Uploads file a buffer or string to the uploads bucket as a file 
const writeFileFromBuffer = async (object, destinationPath) => {
    try{
        destinationPath = clean_file_path(destinationPath);
        await gcs_client.write_file_obj_to_bucket(BUCKET_NAME, destinationPath, object);
        return OK_RESPONSE_TEXT;
    }
    catch(err){
        console.error(`error in file_storage/index.js/writeFileFromBuffer`);
        console.error(err);
        throw err;
    }
}

//Get a writable stream to pipe content to
const getWriteStream = async(filePath) => { 
    try{
        filePath = clean_file_path(filePath);
        return await gcs_client.get_write_stream_for_bucket(BUCKET_NAME, filePath);
    }
    catch(err){
        console.error(`error in file_storage/index.js/writeFileFromBuffer`);
        console.error(err);
        throw err;
    }
}

//Check if a file with the given filePath already exists
const exists = async (filePath) => {
    try{
        if (filePath==null || filePath==undefined) {
            throw new Error(`file_storage/exists: filePath is ${filePath}`);
        }
        filePath = clean_file_path(filePath);
        //check if the key exists
        let response = await gcs_client.check_if_file_exists(
            BUCKET_NAME,
            filePath
        );
        return response.length==1;
    }
    catch(err) {
        console.error(`error in file_storage/index.js/exists`);   
        console.error(err);
        throw err;
    }
}

//Copy a file within the uploads bucket
const copy = async(sourceFilePath, destFilePath) => {
    try{
        //clean the file paths
        sourceFilePath = clean_file_path(sourceFilePath);
        destFilePath = clean_file_path(destFilePath);
        //copy the file
        await gcs_client.copy(BUCKET_NAME, sourceFilePath, BUCKET_NAME, destFilePath);
        return OK_RESPONSE_TEXT;
    }
    catch(err) {
        console.error(`error in file_storage/index.js/copy`);
        console.error(err);
        throw err;
    }
}

//Returns the content of a file
const readFile = async(filePath) => {
    try{
        filePath = clean_file_path(filePath);
        return (await gcs_client.read_file(BUCKET_NAME, filePath)).toString('utf-8');
    }
    catch(err){
        console.error(`error in file_storage/index.js/readFile`);
        console.error(err);
        throw err;
    }
}


module.exports = {
    listFiles, 
    exists,
    copy, 
    getMulter,
    writeFileFromPath,
    writeFileFromBuffer,
    getWriteStream,
    readFile,
    utils
}