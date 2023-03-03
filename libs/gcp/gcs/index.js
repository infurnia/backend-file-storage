const {Storage} = require('@google-cloud/storage');
const moment = require('moment');
class InfurniaGCSClient {
    constructor(project_id, key_file_path) {
        if (!project_id) throw new Error(`InfurniaGCSClient: project_id must be specified`);
        let opts = { projectId: project_id};
        if (key_file_path) opts.keyFilename = key_file_path;
        this.gcsClient = new Storage( opts ); 
    }

    //upload a file object to a single bucket
    write_file_obj_to_bucket = async(bucket_name, key, file_object, content_disposition_attachment=false, large_file=false) => {
        try{
            const gcs_file = this.gcsClient.bucket(bucket_name).file(key);
            if (content_disposition_attachment) {
                await gcs_file.save(file_object, { resumable: large_file, metadata: { contentDisposition: 'attachment'}});
            }
            else {
                await gcs_file.save(file_object, { resumable: large_file });
            }
            return 1;
        }
        catch(err) {
            console.error(err);
            throw err;
        }
    }

    //upload a file from path to a single bucket
    write_file_from_path_to_bucket = async(bucket_name, key, file_path, content_disposition_attachment=false, large_file=false) => {
        try{
            let save_res;
            const gcs_bucket = this.gcsClient.bucket(bucket_name);
            if(content_disposition_attachment){
                save_res = await gcs_bucket.upload(file_path, {destination: key, resumable: large_file, metadata: { contentDisposition: 'attachment'}});
            }
            else {
                save_res = await gcs_bucket.upload(file_path, {destination: key, resumable: large_file});
            }
            return save_res;
        }
        catch(err) {
            console.error(err);
            throw err;
        }
    }

    //get a write stream to write to the bucket
    get_write_stream_for_bucket = async(bucket_name, key, content_disposition_attachment=false) => {
        try{
            if (content_disposition_attachment) {
                return this.gcsClient.bucket(bucket_name).file(key).createWriteStream({ metadata: { contentDisposition: 'attachment' }});
            }
            else {
                return this.gcsClient.bucket(bucket_name).file(key).createWriteStream();
            }
        }
        catch(err) {
            console.error(err);
            throw err;
        }
    }
    
    //list files in bucket
    list_files_in_bucket = async(bucket_name, key_prefix='') => {
        try {
            const gcs_bucket = this.gcsClient.bucket(bucket_name);
            return await gcs_bucket.getFiles({ prefix: key_prefix})
        } catch (err) {
            console.error(err);
            throw err;
        }
    }

    //check if the file exists
    check_if_file_exists = async(bucket_name, key) => {
        try{
            let all_keys = await this.list_files_in_bucket(bucket_name,key);
            return all_keys[0].some(x=> x.name==key);
        }
        catch(err){
            console.error(err);
            throw err;
        }
    }

    //copy files within GCS
    copy = async(source_bucket_name, source_key, destination_bucket_name, destination_key) => {
        try{
            if (!source_bucket_name) throw new Error(`source_bucket must be specified`);
            if (!destination_bucket_name) throw new Error(`source_bucket must be specified`);
            const source_gcs_file = this.gcsClient.bucket(source_bucket_name).file(source_key);
            const destination_gcs_file = this.gcsClient.bucket(destination_bucket_name).file(destination_key);
            return await source_gcs_file.copy(destination_gcs_file);
        }
        catch(err){
            console.error(err);
            throw err;
        }
    }

    //read file contents
    read_file = async(bucket_name, key) => {
        try{
            if (!bucket_name) throw new Error(`bucket_name must be specified`);
            if (!key) throw new Error(`key must be specified`);
            const gcs_file = this.gcsClient.bucket(bucket_name).file(key);
            const res = await gcs_file.download();
            return res[0];
        }
        catch(err){
            console.error(err);
            throw err;
        }
    }

    //read file contents
    download_file = async(bucket_name, key, destination_path) => {
        try{
            if (!bucket_name) throw new Error(`bucket_name must be specified`);
            if (!key) throw new Error(`key must be specified`);
            const gcs_file = this.gcsClient.bucket(bucket_name).file(key);
            await gcs_file.download({ destination: destination_path});
            return "OK";
        }
        catch(err){
            console.error(err);
            throw err;
        }
    }

    //get signed url
    get_signed_url = async(bucket_name, key, permission='read', expiry_in_secs=600) => {
        try{
            if (!bucket_name) throw new Error(`bucket_name must be specified`);
            if (!key) throw new Error(`key must be specified`);
            if (permission!='read' && permission!='write') {
                throw new Error(`permission level ${permission} is not supported, must be one of ['read', 'write']`);
            }
            const gcs_file = this.gcsClient.bucket(bucket_name).file(key);
            let expires = new Date();
            expires = moment(expires).add(expiry_in_secs, 's').toDate();
            const res = await gcs_file.getSignedUrl({action: permission, expires: expires});
            return res[0];
        }
        catch(err){
            console.error(err);
            throw err;
        }
    }
}

module.exports = InfurniaGCSClient;