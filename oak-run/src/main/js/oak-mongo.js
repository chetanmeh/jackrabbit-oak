/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*global print, _, db, Object, ObjectId */

/** @namespace */
var oak = (function(global){
    "use strict";

    var api;

    api = function(){
        print("Oak Mongo Helpers");
    };

    /**
     * Collects various stats related to Oak usage of Mongo.
     *
     * @memberof oak
     * @method oak.systemStats
     * @returns {object} system stats.
     */
    api.systemStats = function () {
        var result = {};
        result.nodeStats = db.nodes.stats(1024 * 1024);
        result.blobStats = db.blobs.stats(1024 * 1024);
        result.clusterStats = db.clusterNodes.find().toArray();
        result.oakIndexes = db.nodes.find({'_id': /^2\:\/oak\:index\//}).toArray();
        result.hostInfo = db.hostInfo();
        result.rootDoc = db.nodes.findOne({'_id' : '0:/'});
        return result;
    };

    /**
     * Collects various stats related to Oak indexes stored under /oak:index.
     *
     * @memberof oak
     * @method indexStats
     * @returns {Array} index stats.
     */
    api.indexStats = function () {
        var result = [];
        var totalCount = 0;
        var totalSize = 0;
        db.nodes.find({'_id': /^2\:\/oak\:index\//}, {_id: 1}).forEach(function (doc) {
            var stats = api.getChildStats(api.pathFromId(doc._id));
            stats.id = doc._id;
            result.push(stats);

            totalCount += stats.count;
            totalSize += stats.size;
        });

        result.push({id: "summary", count: totalCount, size: totalSize, "simple": humanFileSize(totalSize)});
        return result;
    };

    /**
     * Determines the number of child node (including all sub tree)
     * for a given parent node path. This would be faster compared to
     * {@link getChildStats} as it does not load the doc and works on
     * index only.
     *
     * Note that there might be some difference between db.nodes.count()
     * and countChildren('/') as split docs, intermediate docs are not
     * accounted for
     *
     * @memberof oak
     * @method countChildren
     * @param {string} path the path of a node.
     * @returns {number} the number of children, including all descendant nodes.
     */
    api.countChildren = function(path){
        var depth = pathDepth(path);
        var totalCount = 0;
        while (true) {
            var count = db.nodes.count({_id: pathFilter(depth++, path)});
            if( count === 0){
                break;
            }
            totalCount += count;
        }
        return totalCount;
    };

    /**
     * Provides stats related to number of child nodes
     * below given path or total size taken by such nodes.
     *
     * @memberof oak
     * @method getChildStats
     * @param {string} path the path of a node.
     * @returns {{count: number, size: number}} statistics about the child nodes
     *          including all descendants.
     */
    api.getChildStats = function(path){
        var count = 0;
        var size = 0;
        this.forEachChild(path, function(doc){
            count++;
            size +=  Object.bsonsize(doc);
        });
        return {"count" : count, "size" : size, "simple" : humanFileSize(size)};
    };

    /**
     * Performs a breadth first traversal for nodes under given path
     * and invokes the passed function for each child node.
     *
     * @memberof oak
     * @method forEachChild
     * @param {string} path the path of a node.
     * @param callable a function to be called for each child node including all
     *        descendant nodes. The MongoDB document is passed as the single
     *        parameter of the function.
     */
    api.forEachChild = function(path, callable) {
        var depth = pathDepth(path);
        while (true) {
            var cur = db.nodes.find({_id: pathFilter(depth++, path)});
            if(!cur.hasNext()){
                break;
            }
            cur.forEach(callable);
        }
    };

    /**
     * Returns the path part of the given id.
     *
     * @memberof oak
     * @method pathFromId
     * @param {string} id the id of a Document in the nodes collection.
     * @returns {string} the path derived from the id.
     */
    api.pathFromId = function(id) {
        var index = id.indexOf(':');
        return id.substring(index + 1);
    };

    /**
     * Checks the _lastRev for a given clusterId. The checks starts with the
     * given path and walks up to the root node.
     *
     * @memberof oak
     * @method checkLastRevs
     * @param {string} path the path of a node to check
     * @param {number} clusterId the id of an oak cluster node.
     * @returns {object} the result of the check.
     */
    api.checkLastRevs = function(path, clusterId) {
        return checkOrFixLastRevs(path, clusterId, true);
    };

    /**
     * Fixes the _lastRev for a given clusterId. The fix starts with the
     * given path and walks up to the root node.
     *
     * @memberof oak
     * @method fixLastRevs
     * @param {string} path the path of a node to fix
     * @param {number} clusterId the id of an oak cluster node.
     * @returns {object} the result of the fix.
     */
    api.fixLastRevs = function(path, clusterId) {
        return checkOrFixLastRevs(path, clusterId, false);
    };

    /**
     * Returns statistics about the blobs collection in the current database.
     * The stats include the combined BSON size of all documents. The time to
     * run this command therefore heavily depends on the size of the collection.
     *
     * @memberof oak
     * @method blobStats
     * @returns {object} statistics about the blobs collection.
     */
    api.blobStats = function() {
        var result = {};
        var stats = db.blobs.stats(1024 * 1024);
        var bsonSize = 0;
        db.blobs.find().forEach(function(doc){bsonSize += Object.bsonsize(doc)});
        result.count = stats.count;
        result.size = stats.size;
        result.storageSize = stats.storageSize;
        result.bsonSize = Math.round(bsonSize / (1024 * 1024));
        result.indexSize = stats.totalIndexSize;
        return result;
    };

    /**
     * Converts the given Revision String into a more human readable version,
     * which also prints the date.
     *
     * @memberof oak
     * @method formatRevision
     * @param {string} rev a revision string.
     * @returns {string} a human readable string representation of the revision.
     */
    api.formatRevision = function(rev) {
        return new Revision(rev).toReadableString();
    };

    /**
     * Removes the complete subtree rooted at the given path.
     *
     * @memberof oak
     * @method removeDescendantsAndSelf
     * @param {string} path the path of the subtree to remove.
     */
    api.removeDescendantsAndSelf = function(path) {
        var count = 0;
        var depth = pathDepth(path);
        var id = depth + ":" + path;
        // current node at path
        var result = db.nodes.remove({_id: id});
        count += result.nRemoved;
        // might be a long path
        result = db.nodes.remove(longPathQuery(path));
        count += result.nRemoved;
        // descendants
        var prefix = path + "/";
        depth++;
        while (true) {
            result = db.nodes.remove(longPathFilter(depth, prefix));
            count += result.nRemoved;
            result = db.nodes.remove({_id: pathFilter(depth++, prefix)});
            count += result.nRemoved;
            if (result.nRemoved == 0) {
                break;
            }
        }
        // descendants further down the hierarchy with long path
        while (true) {
            result = db.nodes.remove(longPathFilter(depth++, prefix));
            if (result.nRemoved == 0) {
                break;
            }
            count += result.nRemoved;
        }
        return {nRemoved : count};
    };

    /**
     * List all checkpoints.
     *
     * @memberof oak
     * @method listCheckpoints
     * @returns {object} all checkpoints
     */
    api.listCheckpoints = function() {
        var result = {};
        var doc = db.settings.findOne({_id:"checkpoint"});
        if (doc == null) {
            print("No checkpoint document found.");
            return;
        }
        var data = doc.data;
        var r;
        for (r in data) {
            var rev = new Revision(r);
            var exp;
            if (data[r].charAt(0) == '{') {
                exp = JSON.parse(data[r])["expires"];
            } else {
                exp = data[r];
            }
            result[r] = {created:rev.asDate(), expires:new Date(parseInt(exp, 10))};
        }
        return result;
    };

    /**
     * Removes all checkpoints older than a given Revision.
     *
     * @memberof oak
     * @method removeCheckpointsOlderThan
     * @param {string} rev checkpoints older than this revision are removed.
     * @returns {object} the result of the MongoDB update.
     */
    api.removeCheckpointsOlderThan = function(rev) {
        if (rev === undefined) {
            print("No revision specified");
            return;
        }
        var r = new Revision(rev);
        var unset = {};
        var cps = api.listCheckpoints();
        var x;
        var num = 0;
        for (x in cps) {
            if (r.isNewerThan(new Revision(x))) {
                unset["data." + x] = "";
                num++;
            }
        }
        if (num > 0) {
            var update = {};
            update["$inc"] = {_modCount: 1};
            update["$unset"] = unset;
            return db.settings.update({_id:"checkpoint"}, update);
        } else {
            print("No checkpoint older than " + rev);
        }
    };

    /**
     * Removes all collision markers on the document with the given path and
     * clusterId. This method will only remove collisions when the clusterId
     * is inactive.
     *
     * @memberof oak
     * @method removeCollisions
     * @param {string} path the path of a document
     * @param {number} clusterId collision markers for this clusterId will be removed.
     * @returns {object} the result of the MongoDB update.
     */
    api.removeCollisions = function(path, clusterId) {
        if (path === undefined) {
            print("No path specified");
            return;
        }
        if (clusterId === undefined) {
            print("No clusterId specified");
            return;
        }
        // refuse to remove when clusterId is marked active
        var clusterNode = db.clusterNodes.findOne({_id: clusterId.toString()});
        if (clusterNode && clusterNode.state == "ACTIVE") {
            print("Cluster node with id " + clusterId + " is active!");
            print("Can only remove collisions for inactive cluster node.");
            return;
        }

        var doc = this.findOne(path);
        if (!doc) {
            print("No document for path: " + path);
            return;
        }
        var unset = {};
        var r;
        var num = 0;
        for (r in doc._collisions) {
            if (new Revision(r).getClusterId() == clusterId) {
                unset["_collisions." + r] = "";
                num++;
            }
        }
        if (num > 0) {
            var update = {};
            update["$inc"] = {_modCount: 1};
            update["$unset"] = unset;
            return db.nodes.update({_id: pathDepth(path) + ":" + path}, update);
        } else {
            print("No collisions found for clusterId " + clusterId);
        }
    };

    /**
     * Finds the document with the given path.
     *
     * @memberof oak
     * @method findOne
     * @param {string} path the path of the document.
     * @returns {object} the document or null if it doesn't exist.
     */
    api.findOne = function(path) {
        if (path === undefined) {
            return null;
        }
        return db.nodes.findOne({_id: pathDepth(path) + ":" + path});
    };

    /**
     * Checks the history of previous documents at the given path. Orphaned
     * references to removed previous documents are counted and listed when
     * run with verbose set to true.
     *
     * @memberof oak
     * @method checkHistory
     * @param {string} path the path of the document.
     * @param {boolean} [verbose=false] if true, the result object will contain a list
     *        of dangling references to previous documents.
     * @returns {object} the result of the check.
     */
    api.checkHistory = function(path, verbose) {
        return checkOrFixHistory(path, false, verbose);
    };

    /**
     * Repairs the history of previous documents at the given path. Orphaned
     * references to removed previous documents are cleaned up and listed when
     * run with verbose set to true.
     *
     * @memberof oak
     * @method fixHistory
     * @param {string} path the path of the document.
     * @param {boolean} [verbose=false] if true, the result object will contain a list
     *        of removed references to previous documents.
     * @returns {object} the result of the fix.
     */
    api.fixHistory = function(path, verbose) {
        return checkOrFixHistory(path, true, verbose);
    };

    //~--------------------------------------------------< internal >

    var checkOrFixHistory = function(path, fix, verbose) {
        if (path === undefined) {
            print("No path specified");
            return;
        }
        if (path.length > 165) {
            print("Path too long");
            return;
        }

        var doc = api.findOne(path);
        if (!doc) {
            return null;
        }

        var result = {};
        result._id = pathDepth(path) + ":" + path;
        if (verbose) {
            result.prevDocs = [];
            if (fix) {
                result.prevLinksRemoved = [];
            } else {
                result.prevLinksDangling = [];
            }
        }
        result.numPrevDocs = 0;
        if (fix) {
            result.numPrevLinksRemoved = 0;
        } else {
            result.numPrevLinksDangling = 0;
        }


        forEachPrev(doc, function traverse(d, high, low, height) {
            var p = "p" + path;
            if (p.charAt(p.length - 1) != "/") {
                p += "/";
            }
            p += high + "/" + height;
            var id = (pathDepth(path) + 2) + ":" + p;
            var prev = db.nodes.findOne({_id: id });
            if (prev) {
                if (result.prevDocs) {
                    result.prevDocs.push(high + "/" + height);
                }
                result.numPrevDocs++;
                if (parseInt(height) > 0) {
                    forEachPrev(prev, traverse);
                }
            } else if (fix) {
                if (result.prevLinksRemoved) {
                    result.prevLinksRemoved.push(high + "/" + height);
                }
                result.numPrevLinksRemoved++;
                var update = {};
                update.$inc = {_modCount : 1};
                if (d._sdType == 40) { // intermediate split doc type
                    update.$unset = {};
                    update.$unset["_prev." + high] = 1;
                } else {
                    update.$set = {};
                    update.$set["_stalePrev." + high] = height;
                }
                db.nodes.update({_id: d._id}, update);
            } else {
                if (result.prevLinksDangling) {
                    result.prevLinksDangling.push(high + "/" + height);
                }
                result.numPrevLinksDangling++;
            }
        });
        return result;
    };

    var forEachPrev = function(doc, callable) {
        var stalePrev = doc._stalePrev;
        if (!stalePrev) {
            stalePrev = {};
        }
        var r;
        for (r in doc._prev) {
            var value = doc._prev[r];
            var idx = value.lastIndexOf("/");
            var height = value.substring(idx + 1);
            var low = value.substring(0, idx);
            if (stalePrev[r] == height) {
                continue;
            }
            callable.call(this, doc, r, low, height);
        }
    };

    var checkOrFixLastRevs = function(path, clusterId, dryRun) {
         if (path === undefined) {
            print("Need at least a path from where to start check/fix.");
            return;
         }
         var result = [];
         var lastRev;
         if (path.length == 0 || path.charAt(0) != '/') {
            return "Not a valid absolute path";
         }
         if (clusterId === undefined) {
            clusterId = 1;
         }
         while (true) {
            var doc = db.nodes.findOne({_id: pathDepth(path) + ":" + path});
            if (doc) {
                var revStr = doc._lastRev["r0-0-" + clusterId];
                if (revStr) {
                    var rev = new Revision(revStr);
                    if (lastRev && lastRev.isNewerThan(rev)) {
                        if (dryRun) {
                            result.push({_id: doc._id, _lastRev: rev.toString(), needsFix: lastRev.toString()});
                        } else {
                            var update = {$set:{}};
                            update.$set["_lastRev.r0-0-" + clusterId] = lastRev.toString();
                            db.nodes.update({_id: doc._id}, update);
                            result.push({_id: doc._id, _lastRev: rev.toString(), fixed: lastRev.toString()});
                        }
                    } else {
                        result.push({_id: doc._id, _lastRev: rev.toString()});
                        lastRev = rev;
                    }
                }
            }
            if (path == "/") {
                break;
            }
            var idx = path.lastIndexOf("/");
            if (idx == 0) {
                path = "/";
            } else {
                path = path.substring(0, idx);
            }
         }
         return result;
    };

    var Revision = function(rev) {
        var dashIdx = rev.indexOf("-");
        this.rev = rev;
        this.timestamp = parseInt(rev.substring(1, dashIdx), 16);
        this.counter = parseInt(rev.substring(dashIdx + 1, rev.indexOf("-", dashIdx + 1)), 16);
        this.clusterId = parseInt(rev.substring(rev.lastIndexOf("-") + 1), 16);
    };

    Revision.prototype.toString = function () {
        return this.rev;
    };

    Revision.prototype.isNewerThan = function(other) {
        if (this.timestamp > other.timestamp) {
            return true;
        } else if (this.timestamp < other.timestamp) {
            return false;
        } else {
            return this.counter > other.counter;
        }
    };

    Revision.prototype.toReadableString = function () {
        return this.rev + " (" + this.asDate().toString() + ")"
    };

    Revision.prototype.asDate = function() {
        return new Date(this.timestamp);
    };

    Revision.prototype.getClusterId = function() {
        return this.clusterId;
    };

    var pathDepth = function(path){
        if(path === '/'){
            return 0;
        }
        var depth = 0;
        for(var i = 0; i < path.length; i++){
            if(path.charAt(i) === '/'){
                depth++;
            }
        }
        return depth;
    };

    var pathFilter = function (depth, prefix){
        return new RegExp("^"+ depth + ":" + prefix);
    };

    var longPathFilter = function (depth, prefix) {
        var filter = {};
        filter._id = new RegExp("^" + depth + ":h");
        filter._path = new RegExp("^" + prefix);
        return filter;
    };

    var longPathQuery = function (path) {
        var query = {};
        query._id = new RegExp("^" + pathDepth(path) + ":h");
        query._path = path;
        return query;
    };

    //http://stackoverflow.com/a/20732091/1035417
    var humanFileSize = function (size) {
        var i = Math.floor( Math.log(size) / Math.log(1024) );
        return ( size / Math.pow(1024, i) ).toFixed(2) * 1 + ' ' + ['B', 'kB', 'MB', 'GB', 'TB'][i];
    };

    return api;
}(this));