const {FileStorage} = require('./index');
let file_storage = new FileStorage({ bucket_name: 'infurnia-dev-uploads', project_id: 'infurnia-stage-vitess'});


var main = async() => {
    try {
        console.log(await file_storage.exists('prom_logo.png'));
        console.log(await file_storage.readFile('prom_logo.png'));
        return 'OK';
    }
    catch(err){
        console.error(err);
        throw err;       
    }
}

main().then(x=> { console.log(x); process.exit(0);}).catch(err=> { console.error(err); process.exit(0)})