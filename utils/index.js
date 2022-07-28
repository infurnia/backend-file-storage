const {relative} = require('path');
const readdir = require('fs').promises.readdir;

const clean_file_path = function(filePath) {
    if (filePath==null || filePath==undefined){
        console.warn(`filePath is null or undefined`);
        return null;
    }
    if (filePath.length<2) {
        if (filePath[0]!='/' && filePath[1]!='.') {
            return filePath;
        }
        else {
            throw new Error(`filePath ${filePath} is not supported for file_storage`);
        }
    }
    //clean the filePath of ./ stuff
    if (filePath[0]=='.' && filePath[1]=='/'){
        filePath = filePath.slice(2);
    }
    else if (filePath[0]=='/'){
        filePath = filePath.slice(1);
    }
    return filePath;
}

const _getFileList = async (dirPath, originalDirPath, recursive=true, removeParentDirPrefix=true) => {
    let files = [];
    const items = await readdir(dirPath, { withFileTypes: true });
    for (let item of items) {
        if (recursive && item.isDirectory()) {
            files = [
                ...files,
                ...(await _getFileList(`${dirPath}/${item.name}`, originalDirPath, recursive, removeParentDirPrefix)),
            ];
        } else if(!item.isDirectory()) {
            if (removeParentDirPrefix) {
                files.push(relative(originalDirPath, `${dirPath}/${item.name}`));
            }
            else {
                files.push(`${dirPath}/${item.name}`);
            }
        }
    }

    return files;
};

const getFileList = async (dirPath, recursive=true, removeParentDirPrefix=true) => {
    return await _getFileList(dirPath, dirPath, recursive, removeParentDirPrefix);
};

exports.clean_file_path = clean_file_path;
exports.getFileList = getFileList;