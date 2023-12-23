const utils = require('./utils');
const clean_file_path = utils.clean_file_path;
const InfurniaGCSClient = require('./libs/gcp/gcs');
const multerGoogleStorage = require("@infurnia/infurnia-multer-cloud-storage");
const OK_RESPONSE_TEXT = "OK";
const crypto = require('crypto');
const path = require('path');
const fs_promises = require('fs').promises;

class FileStorage {
    constructor({project_id, bucket_name}) {
        this.project_id = project_id || process.env.GCP_PROJECT_ID;
        this.bucket_name = bucket_name || process.env.GCS_FILE_STORAGE_BUCKET_NAME;
        if (!this.project_id) { 
            throw new Error(`neither project_id is given to the FileStorage constructor, nor is env var GCP_PROJECT_ID is not set`);
        }
        if (!this.bucket_name) {
            throw new Error(`neither bucket_name is given to the FileStorage constructor, nor is env var GCS_FILE_STORAGE_BUCKET_NAME is not set`);
        }
        this.gcs_client = new InfurniaGCSClient(this.project_id, null);
    }

    //returns multer storage engine object for the bucket
    getMulter = function(
        destination,
        filename_fn = function (req, file, cb) {
            crypto.pseudoRandomBytes(16, function (err, raw) {
                if (err) return cb(err);
                cb(null, raw.toString('hex') + path.extname(file.originalname));
            })
        }
    ) {
        try{
            let bucket_name = this.bucket_name;
            let project_id = this.project_id;
            return multerGoogleStorage.storageEngine({
                bucket: bucket_name,
                projectId: project_id,
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

    //returns the list of files in the directory
    listFiles = async (directory) => {
        try{
            let files_list = await this.gcs_client.list_files_in_bucket(this.bucket_name, directory);
            return files_list[0].map(x=> { return { name: x.name, created_at: x.metadata.timeCreated, updated_at: x.metadata.updated}});
        }
        catch(err){
            console.error(`error in file_storage/index.js/listFiles`);
            console.error(err);
            throw err;
        }
    }

    //Uploads file from a path to the main uploads bucket
    writeFileFromPath = async (sourcePath, destinationPath, deleteSourceFileAfterUpload=false, contentDispositionAttachment=false) => {
        //by default will upload to one bucket only...change priorityLevel to change
        try{
            destinationPath = clean_file_path(destinationPath, false);
            await this.gcs_client.write_file_from_path_to_bucket(this.bucket_name, destinationPath, sourcePath, contentDispositionAttachment);
            if (deleteSourceFileAfterUpload===true) {
                await fs_promises.rm(sourcePath);
            }
            else if (typeof deleteSourceFileAfterUpload==='string' && path.relative(deleteSourceFileAfterUpload, sourcePath).includes('/')==false){
                //the path to be deleted can only be one level above the path being copied at max...so there is no slash in the relative path
                await fs_promises.rm(deleteSourceFileAfterUpload, {recursive: true});
            }
            return OK_RESPONSE_TEXT;
        }   
        catch(err){
            console.error(`error in file_storage/index.js/writeFileFromPath`);
            console.error(err);
            throw err;
        }
    }

    //Uploads file a buffer or string to the uploads bucket as a file 
    writeFileFromBuffer = async (object, destinationPath, contentDispositionAttachment=false) => {
        try{
            destinationPath = clean_file_path(destinationPath, false);
            await this.gcs_client.write_file_obj_to_bucket(this.bucket_name, destinationPath, object, contentDispositionAttachment);
            return OK_RESPONSE_TEXT;
        }
        catch(err){
            console.error(`error in file_storage/index.js/writeFileFromBuffer`);
            console.error(err);
            throw err;
        }
    }
    
    //Get a writable stream to pipe content to
    getWriteStream = async(filePath, contentDispositionAttachment=false) => { 
        try{
            filePath = clean_file_path(filePath, false);
            return await this.gcs_client.get_write_stream_for_bucket(this.bucket_name, filePath, contentDispositionAttachment);
        }
        catch(err){
            console.error(`error in file_storage/index.js/writeFileFromBuffer`);
            console.error(err);
            throw err;
        }
    }

    //Check if a file with the given filePath already exists
    exists = async (filePath) => {
        try{
            if (filePath==null || filePath==undefined) {
                throw new Error(`file_storage/exists: filePath is ${filePath}`);
            }
            filePath = clean_file_path(filePath, false);
            //check if the key exists
            return await this.gcs_client.check_if_file_exists(
                this.bucket_name,
                filePath
            );
        }
        catch(err) {
            console.error(`error in file_storage/index.js/exists`);   
            console.error(err);
            throw err;
        }
    }

    //copies a file within the bucket
    copy = async(sourceFilePath, destFilePath) => {
        try{
            //clean the file paths
            sourceFilePath = clean_file_path(sourceFilePath, false);
            destFilePath = clean_file_path(destFilePath, false);
            //copy the file
            await this.gcs_client.copy(this.bucket_name, sourceFilePath, this.bucket_name, destFilePath);
            return OK_RESPONSE_TEXT;
        }
        catch(err) {
            console.error(`error in file_storage/index.js/copy`);
            console.error(err);
            throw err;
        }
    }

    //reads a file from the bucket into a string
    readFile = async(filePath) => {
        try{
            filePath = clean_file_path(filePath, false);
            return (await this.gcs_client.read_file(this.bucket_name, filePath)).toString('utf-8');
        }
        catch(err){
            console.error(`error in file_storage/index.js/readFile`);
            console.error(err);
            throw err;
        }
    }

    //downloads gcs object as a file
    downloadFile = async(sourceFilePath, destFilePath) => {
        try{
            sourceFilePath = clean_file_path(sourceFilePath, false);
            console.debug(sourceFilePath, destFilePath);
            return (await this.gcs_client.download_file(this.bucket_name, sourceFilePath, destFilePath));
        }
        catch(err){
            console.error(`error in file_storage/index.js/downloadFile`);
            console.error(err);
            throw err;
        }
    }

    //gets a signed url for an object with read permission
    getSignedUrl = async(filePath) => {
        try{
            filePath = clean_file_path(filePath, false);
            return await this.gcs_client.get_signed_url(this.bucket_name, filePath, 'read');
        }
        catch(err){
            console.error(`error in file_storage/index.js/getSignedUrl`);
            console.error(err);
            throw err;
        }
    }
}

module.exports = {
    FileStorage,
    utils
}