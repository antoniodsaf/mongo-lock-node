"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
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
// Helper function that converts setTimeout to a Promise
function timeoutPromise(delay) {
    return new Promise(function (resolve) {
        setTimeout(function () {
            resolve();
        }, delay);
    });
}
/*
 * RWMutex implements a distributed reader/writer lock backed by mongodb. Right now it is limited
 * in a few key ways:
 * 1. Re-enterable locks. RWMutex treats a clientID already existing on the lock in the db as a
 *    lock that this client owns. It re-enters the lock and proceeds as if you have the lock.
 * 2. No heartbeat. Our current requirements for this project do not include heartbeats, so any
 *    client that does not call unlock will remain on the lock forever.
 * 3. Manual setup. This library does not currently support setup. The collection you pass to the
 *    constructor must have a unique index on the `lockID` field.
 */
var RWMutex = /** @class */ (function () {
    /*
     * Creates a new RWMutex
     * @param {mongodb Collection} collection - the mongodb Collection where the object should be stored
     * @param {string} lockID - id corresponding to the resource you are locking. Must be unique
     * @param {string} clientID - id corresponding to the client using this lock instance. Must be unique
     * @param {Object} options - lock options
     */
    function RWMutex(coll, lockID, clientID, options) {
        if (options === void 0) { options = { sleepTime: 5000, maximumRetry: 3 }; }
        this._coll = coll;
        this._lockID = lockID;
        this._clientID = clientID;
        this._options = options;
    }
    /*
     * Acquires the write lock.
     * @return {Promise} - Promise that resolves when the lock is acquired, rejects if an error occurs
     */
    RWMutex.prototype.lock = function () {
        return __awaiter(this, void 0, void 0, function () {
            var lock, retry, result, err_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._findOrCreateLock()];
                    case 1:
                        lock = _a.sent();
                        // if this clientID already has the lock, we re-enter the lock and return
                        if (lock.writer === this._clientID) {
                            return [2 /*return*/];
                        }
                        retry = 0;
                        _a.label = 2;
                    case 2:
                        if (!true) return [3 /*break*/, 8];
                        _a.label = 3;
                    case 3:
                        _a.trys.push([3, 5, , 6]);
                        return [4 /*yield*/, this._coll.updateOne({
                                lockID: this._lockID,
                                readers: [],
                                writer: "",
                            }, {
                                $set: {
                                    writer: this._clientID,
                                },
                            })];
                    case 4:
                        result = _a.sent();
                        if (result.matchedCount > 0) {
                            return [2 /*return*/];
                        }
                        return [3 /*break*/, 6];
                    case 5:
                        err_1 = _a.sent();
                        throw new Error("error aquiring lock " + this._lockID + ": " + err_1.message);
                    case 6:
                        if (++retry == this._options.maximumRetry) {
                            console.info("maximum retry reached, skipped");
                            return [2 /*return*/];
                        }
                        return [4 /*yield*/, timeoutPromise(this._options.sleepTime)];
                    case 7:
                        _a.sent();
                        return [3 /*break*/, 2];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    /*
     * Unlocks the write lock. Must have the same lock type
     * @return {Promise} - Resolves when lock is released, rejects if an error occurs
     */
    RWMutex.prototype.unlock = function () {
        return __awaiter(this, void 0, void 0, function () {
            var result, err_2;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this._coll.updateOne({
                                lockID: this._lockID,
                                writer: this._clientID,
                            }, {
                                $set: {
                                    writer: "",
                                },
                            })];
                    case 1:
                        result = _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_2 = _a.sent();
                        throw new Error("error releasing lock " + this._lockID + ": " + err_2.message);
                    case 3:
                        if (result.matchedCount === 0) {
                            throw new Error("lock " + this._lockID + " not currently held by client: " + this._clientID);
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /*
     * Acquires the read lock.
     * @return {Promise} - Resolves when the lock is acquired, rejects if an error occurs
     */
    RWMutex.prototype.rLock = function () {
        return __awaiter(this, void 0, void 0, function () {
            var lock, result, err_3;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this._findOrCreateLock()];
                    case 1:
                        lock = _a.sent();
                        if (lock.readers.indexOf(this._clientID) > -1) {
                            // if this clientID is already a reader, we can re-enter the lock here
                            return [2 /*return*/];
                        }
                        _a.label = 2;
                    case 2:
                        if (!true) return [3 /*break*/, 8];
                        _a.label = 3;
                    case 3:
                        _a.trys.push([3, 5, , 6]);
                        return [4 /*yield*/, this._coll.updateOne({
                                lockID: this._lockID,
                                writer: "",
                            }, {
                                $addToSet: {
                                    readers: this._clientID,
                                },
                            })];
                    case 4:
                        result = _a.sent();
                        // We check matchedCount rather than modifiedCount here to make the lock re-enterable.
                        // If the lock should not be re-enterable, or clientIDs are not unique, this
                        // implemenation will break.
                        // TODO: option to make it not re-enterable
                        if (result.matchedCount > 0) {
                            return [2 /*return*/];
                        }
                        return [3 /*break*/, 6];
                    case 5:
                        err_3 = _a.sent();
                        throw new Error("error aquiring lock " + this._lockID + ": " + err_3.message);
                    case 6: return [4 /*yield*/, timeoutPromise(this._options.sleepTime)];
                    case 7:
                        _a.sent();
                        return [3 /*break*/, 2];
                    case 8: return [2 /*return*/];
                }
            });
        });
    };
    /*
     * Unlocks the read lock. Must have the same lock type
     * @return {Promise} - Resolves when lock is released, rejects if an error occurs
     */
    RWMutex.prototype.rUnlock = function () {
        return __awaiter(this, void 0, void 0, function () {
            var result, err_4;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this._coll.updateOne({
                                lockID: this._lockID,
                                readers: this._clientID,
                            }, {
                                $pull: {
                                    readers: this._clientID,
                                },
                            })];
                    case 1:
                        result = _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_4 = _a.sent();
                        throw new Error("error releasing lock " + this._lockID + ": " + err_4.message);
                    case 3:
                        if (result.matchedCount === 0) {
                            throw new Error("lock " + this._lockID + " not currently held by client: " + this._clientID);
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    /*
     * Finds or creates the resource in the mongo collection with the specified lockID
     * @param {string} - unique id of the resource
     * @return {Promise} - resolves with the lock object, rejects with the formatted error
     */
    RWMutex.prototype._findOrCreateLock = function () {
        return __awaiter(this, void 0, void 0, function () {
            var lock, err_5, err_6, err_7;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _a.trys.push([0, 2, , 3]);
                        return [4 /*yield*/, this._coll.find({ lockID: this._lockID }).limit(1).next()];
                    case 1:
                        lock = _a.sent();
                        return [3 /*break*/, 3];
                    case 2:
                        err_5 = _a.sent();
                        throw new Error("error finding lock " + this._lockID + ": " + err_5.message);
                    case 3:
                        if (!!lock) return [3 /*break*/, 7];
                        // lock doesn't exist yet, so we should create it
                        lock = {
                            lockID: this._lockID,
                            readers: [],
                            writer: "",
                        };
                        _a.label = 4;
                    case 4:
                        _a.trys.push([4, 6, , 7]);
                        return [4 /*yield*/, this._coll.insert(lock)];
                    case 5:
                        _a.sent();
                        return [3 /*break*/, 7];
                    case 6:
                        err_6 = _a.sent();
                        // Check for E11000 duplicate key error, which means someone inserted the lock before us
                        if (err_6.code === 11000) {
                            // set the lock to null
                            lock = null;
                        }
                        else {
                            throw new Error("error creating lock for " + this._lockID + ": " + err_6.message);
                        }
                        return [3 /*break*/, 7];
                    case 7:
                        if (!!lock) return [3 /*break*/, 11];
                        _a.label = 8;
                    case 8:
                        _a.trys.push([8, 10, , 11]);
                        return [4 /*yield*/, this._coll.find({ lockID: this._lockID }).limit(1).next()];
                    case 9:
                        lock = _a.sent();
                        return [3 /*break*/, 11];
                    case 10:
                        err_7 = _a.sent();
                        throw new Error("error finding existing lock " + this._lockID + ": " + err_7.message);
                    case 11:
                        if (!lock) {
                            // this should never happen
                            throw new Error("error finding and creating lock " + this._lockID);
                        }
                        return [2 /*return*/, lock];
                }
            });
        });
    };
    return RWMutex;
}());
exports.default = RWMutex;
