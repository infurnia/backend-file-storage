const path = require('path');
const {FileStorage} = require('./index');
let file_storage = new FileStorage({ bucket_name: 'infurnia-dev-uploads', project_id: 'infurnia-stage-vitess'});


var main = async() => {
    try {
        for (let i=0; i<30; i++){
            console.log(await file_storage.writeFileFromPath('sample_vide.avi', `vid_large_noresume_${i}.avi`, false, true, false));
        }
        return 'OK';
    }
    catch(err){
        console.error(err);
        throw err;       
    }
}

main().then(x=> { console.log(x); process.exit(0);}).catch(err=> { console.error(err); process.exit(0)})