"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.storageEngine = void 0;
var storage_1 = require("@google-cloud/storage");
var uuid_1 = require("uuid");
var urlencode = require("urlencode");
var MulterGoogleCloudStorage = /** @class */ (function () {
    function MulterGoogleCloudStorage(opts) {
        var _this = this;
        this._handleFile = function (req, file, cb) { return __awaiter(_this, void 0, void 0, function () {
            var blobFile, blobName, blob, streamOpts, contentType, blobStream;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getBlobFileReference(req, file)];
                    case 1:
                        blobFile = _a.sent();
                        if (blobFile !== false) {
                            blobName = blobFile.destination + blobFile.filename;
                            blob = this.gcsBucket.file(blobName);
                            streamOpts = {};
                            if (!this.options.uniformBucketLevelAccess) {
                                streamOpts.predefinedAcl = this.options.acl || 'private';
                            }
                            contentType = this.getContentType(req, file);
                            if (contentType) {
                                streamOpts.metadata = { contentType: contentType };
                            }
                            blobStream = blob.createWriteStream(streamOpts);
                            file.stream.pipe(blobStream)
                                .on('error', function (err) { return cb(err); })
                                .on('finish', function (file) {
                                var name = blob.metadata.name;
                                var filename = name.substr(name.lastIndexOf('/') + 1);
                                cb(null, {
                                    bucket: blob.metadata.bucket,
                                    destination: blobFile.destination,
                                    filename: filename,
                                    path: "" + blobFile.destination + filename,
                                    contentType: blob.metadata.contentType,
                                    size: blob.metadata.size,
                                    uri: "gs://" + blob.metadata.bucket + "/" + blobFile.destination + filename,
                                    linkUrl: "https://storage.googleapis.com/" + blob.metadata.bucket + "/" + blobFile.destination + filename,
                                    selfLink: blob.metadata.selfLink,
                                    //metadata: blob.metadata
                                });
                            });
                        }
                        return [2 /*return*/];
                }
            });
        }); };
        this._removeFile = function (req, file, cb) { return __awaiter(_this, void 0, void 0, function () {
            var blobFile, blobName, blob;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.getBlobFileReference(req, file)];
                    case 1:
                        blobFile = _a.sent();
                        if (blobFile !== false) {
                            blobName = blobFile.destination + blobFile.filename;
                            blob = this.gcsBucket.file(blobName);
                            blob.delete();
                        }
                        return [2 /*return*/];
                }
            });
        }); };
        opts = opts || {};
        typeof opts.destination === 'string' ?
            this.getDestination = function (req, file, cb) { cb(null, opts.destination); }
            : this.getDestination = opts.destination || this.getDestination;
        if (opts.hideFilename) {
            this.getFilename = function (req, file, cb) { cb(null, "" + uuid_1.v4()); };
            this.getContentType = function (req, file) { return undefined; };
        }
        else {
            typeof opts.filename === 'string' ?
                this.getFilename = function (req, file, cb) { cb(null, opts.filename); }
                : this.getFilename = opts.filename || this.getFilename;
            typeof opts.contentType === 'string' ?
                this.getContentType = function (req, file) { return opts.contentType; }
                : this.getContentType = opts.contentType || this.getContentType;
        }
        opts.bucket = opts.bucket || process.env.GCS_BUCKET || null;
        opts.projectId = opts.projectId || process.env.GCLOUD_PROJECT || null;
        opts.keyFilename = opts.keyFilename || process.env.GCS_KEYFILE || null;
        if (!opts.bucket) {
            throw new Error('You have to specify bucket for Google Cloud Storage to work.');
        }
        if (!opts.projectId) {
            throw new Error('You have to specify project id for Google Cloud Storage to work.');
        }
        // if (!opts.keyFilename && !opts.credentials) {

        //     // throw new Error('You have to specify credentials key file or credentials object, for Google Cloud Storage to work.');
        // }
        if (!opts.keyFilename && !opts.credentials) {
            this.gcsStorage = new storage_1.Storage({
                projectId: opts.projectId,
                keyFilename: opts.keyFilename,
                credentials: opts.credentials
            });
        }
        else {
            console.warn(`Initializing @google/storage without keyFile or any explicit credentials`);
            this.gcsStorage = new storage_1.Storage({
                projectId: opts.projectId
            });
        }
        this.gcsBucket = this.gcsStorage.bucket(opts.bucket);
        this.options = opts;
    }
    //private blobFile: {destination?: string, filename: string} = { destination: '', filename: '' };
    MulterGoogleCloudStorage.prototype.getFilename = function (req, file, cb) {
        if (typeof file.originalname === 'string')
            cb(null, file.originalname);
        else
            cb(null, "" + uuid_1.v4());
    };
    MulterGoogleCloudStorage.prototype.getDestination = function (req, file, cb) {
        cb(null, '');
    };
    MulterGoogleCloudStorage.prototype.getContentType = function (req, file) {
        if (typeof file.mimetype === 'string')
            return file.mimetype;
        else
            return undefined;
    };
    MulterGoogleCloudStorage.prototype.getBlobFileReference = function (req, file) {
        return __awaiter(this, void 0, void 0, function () {
            var blobFile, file_name_set_promise;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        blobFile = {
                            destination: '',
                            filename: '',
                        };
                        this.getDestination(req, file, function (err, destination) {
                            if (err) {
                                return false;
                            }
                            var escDestination = '';
                            escDestination += destination
                                .replace(/^\.+/g, '')
                                .replace(/^\/+|\/+$/g, '');
                            if (escDestination !== '') {
                                escDestination = escDestination + '/';
                            }
                            blobFile.destination = escDestination;
                        });
                        file_name_set_promise = new Promise(function (resolve, reject) {
                            _this.getFilename(req, file, function (err, filename) {
                                if (err) {
                                    return reject(false);
                                }
                                blobFile.filename = urlencode(filename
                                    .replace(/^\.+/g, '')
                                    .replace(/^\/+/g, '')
                                    .replace(/\r|\n/g, '_'));
                                return resolve(blobFile);
                            });
                        });
                        return [4 /*yield*/, file_name_set_promise];
                    case 1:
                        _a.sent();
                        return [2 /*return*/, blobFile];
                }
            });
        });
    };
    return MulterGoogleCloudStorage;
}());
exports.default = MulterGoogleCloudStorage;
function storageEngine(opts) {
    return new MulterGoogleCloudStorage(opts);
}
exports.storageEngine = storageEngine;
//# sourceMappingURL=index.js.map