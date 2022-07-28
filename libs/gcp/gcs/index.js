const {Storage} = require('@google-cloud/storage');

class InfurniaGCSClient {
    constructor(project_id, key_file_path) {
        if (!project_id) throw new Error(`InfurniaGCSClient: project_id must be specified`);
        let opts = { projectId: project_id};
        if (key_file_path) opts.keyFilename = key_file_path;
        this.gcsClient = new Storage( opts ); 
    }

    //upload a file object to a single bucket
    write_file_obj_to_bucket = async(bucket_name, key, file_object) => {
        try{
            const gcs_file = this.gcsClient.bucket(bucket_name).file(key);
            await gcs_file.save(file_object, { resumable: false });
            return 1;
        }
        catch(err) {
            console.error(err);
            throw err;
        }
    }

    //upload a file from path to a single bucket
    write_file_from_path_to_bucket = async(bucket_name, key, file_path) => {
        try{
            const gcs_bucket = this.gcsClient.bucket(bucket_name);
            const save_res = await gcs_bucket.upload(file_path, {destination: key, resumable: false});
            return save_res;
        }
        catch(err) {
            console.error(err);
            throw err;
        }
    }

    //get a write stream to write to the bucket
    get_write_stream_for_bucket = async(bucket_name, key) => {
        try{
            return this.gcsClient.bucket(bucket_name).file(key).createWriteStream();
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
    check_if_file_exists = async(bucket_name, key_prefix) => {
        try{
            let all_keys = await this.list_files_in_bucket(bucket_name,key_prefix);
            return all_keys[0].map(x=>x.name);
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
}

module.exports = InfurniaGCSClient;