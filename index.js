// custom redis
const net = require("net");
const fs = require("fs");
const path = require("path");
const Parser = require("redis-parser");
const http = require("http");
const https = require("https");

const DATA_FILE = path.join(__dirname, "redis_data.json");
const LOG_FILE = path.join(__dirname, "redis_wal.log");

// In-memory stores
let store = {}; // key -> value (strings, arrays, objects)
let expiryTimes = {}; // key -> timestamp ms
let fetchCache = {}; // url -> { data, expiresAt }
let subscribers = new Map(); // channel -> Set(clientID)
let transactions = new Map(); // connection -> queued commands (array)
let clients = new Map(); // clientID -> {id, connection, connectedAt, commandsExecuted, subscribedChannels}
let clientRecords = new Map(); // clientID -> [{time, command, type, key?}]
let clientCounter = 0;

let totalCommands = 0;
let replicaOf = null; // {host, port} simulation (no real replication, just metadata)

// Load persisted file if exists
function loadData() {
  if (fs.existsSync(DATA_FILE)) {
    try {
      const content = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
      store = content.store || {};
      expiryTimes = content.expiryTimes || {};
      fetchCache = content.fetchCache || {};
      console.log("✅ Data loaded from disk.");
    } catch (e) {
      console.error("❌ Failed to load data:", e);
    }
  }
}
loadData();

function saveToFile() {
  try {
    fs.writeFileSync(
      DATA_FILE,
      JSON.stringify({ store, expiryTimes, fetchCache }, null, 2)
    );
  } catch (e) {
    console.error("❌ Save failed:", e);
  }
}

function walLog(line) {
  try {
    fs.appendFileSync(LOG_FILE, `${new Date().toISOString()} ${line}\n`);
  } catch (e) {
    console.error("WAL write err:", e);
  }
}

// Utility: RESP bulk string
function bulkString(str) {
  if (str === null || str === undefined) return "$-1\r\n";
  const s = String(str);
  return `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
}

// Utility: RESP array of bulk strings from JS array
function arrayOfBulk(arr) {
  if (!Array.isArray(arr)) return "*0\r\n";
  return `*${arr.length}\r\n` + arr.map((x) => bulkString(x)).join("");
}

// TTL expiry checker
function checkExpiry() {
  const now = Date.now();
  let changed = false;
  // keys expiry
  for (const k of Object.keys(expiryTimes)) {
    if (now > expiryTimes[k]) {
      delete store[k];
      delete expiryTimes[k];
      changed = true;
    }
  }
  // fetchCache expiry
  for (const url of Object.keys(fetchCache)) {
    if (fetchCache[url].expiresAt && now > fetchCache[url].expiresAt) {
      delete fetchCache[url];
      changed = true;
    }
  }
  if (changed) saveToFile();
}
setInterval(checkExpiry, 5000);

// Event hooks (simple)
const hooks = {
  onSet: (key, value, clientID) => {},
  onDel: (key, clientID) => {},
  onPublish: (channel, message, clientID) => {},
};

// Create server
const server = net.createServer((connection) => {
  const clientID = ++clientCounter;
  clients.set(clientID, {
    id: clientID,
    connection,
    connectedAt: Date.now(),
    commandsExecuted: 0,
    subscribedChannels: new Set(),
    remoteAddress: connection.remoteAddress + ":" + connection.remotePort,
  });
  clientRecords.set(clientID, []);

  console.log(
    `🟢 Client ${clientID} connected (${connection.remoteAddress}:${connection.remotePort})`
  );
  connection.write(`+WELCOME ClientID:${clientID}\r\n`);

  // helper to record per-client history
  function recordClient(cmdArr, meta = {}) {
    const rec = { time: Date.now(), command: cmdArr, ...meta };
    clientRecords.get(clientID).push(rec);
  }

  // On data: parse Redis RESP using redis-parser
  connection.on("data", (data) => {
    const parser = new Parser({
      returnReply: (reply) => {
        totalCommands++;
        clients.get(clientID).commandsExecuted++;
        const cmd = String(reply[0]).toLowerCase();

        recordClient(reply);

        // If client is in MULTI queue (transaction) and command is not multi/exec/discard
        if (
          transactions.has(connection) &&
          !["multi", "exec", "discard"].includes(cmd)
        ) {
          transactions.get(connection).push(reply);
          return connection.write("+QUEUED\r\n");
        }

        // Write handlers
        const sendError = (msg) => connection.write(`-ERR ${msg}\r\n`);
        const sendOK = () => connection.write("+OK\r\n");
        const sendInt = (n) => connection.write(`:${n}\r\n`);

        try {
          switch (cmd) {
            // -------------------------
            // BASIC
            // -------------------------
            case "set": {
              const [, key, value] = reply;
              // If value looks like JSON and starts with { or [ parse it, else store string
              let stored = value;
              try {
                if (
                  typeof value === "string" &&
                  (value.trim().startsWith("{") || value.trim().startsWith("["))
                ) {
                  stored = JSON.parse(value);
                }
              } catch {}
              store[key] = stored;
              delete expiryTimes[key];
              saveToFile();
              walLog(`SET ${key}`);
              hooks.onSet(key, stored, clientID);
              recordClient(reply, { type: "write", key });
              return sendOK();
            }

            case "get": {
              const key = reply[1];
              const val = store[key];
              if (val === undefined) return connection.write("$-1\r\n");
              if (typeof val === "object") {
                const str = JSON.stringify(val);
                return connection.write(bulkString(str));
              } else {
                return connection.write(bulkString(val));
              }
            }

            case "del": {
              let count = 0;
              // allow multiple keys (DEL key1 key2 ...)
              for (let i = 1; i < reply.length; i++) {
                const key = reply[i];
                if (store.hasOwnProperty(key)) {
                  delete store[key];
                  delete expiryTimes[key];
                  count++;
                  hooks.onDel(key, clientID);
                  recordClient(["del", key], { type: "del", key });
                  walLog(`DEL ${key}`);
                }
              }
              saveToFile();
              return sendInt(count);
            }

            case "exists": {
              const key = reply[1];
              return sendInt(store.hasOwnProperty(key) ? 1 : 0);
            }

            case "incr": {
              const key = reply[1];
              const num = parseInt(store[key]) || 0;
              const newv = num + 1;
              store[key] = String(newv);
              saveToFile();
              walLog(`INCR ${key}`);
              return sendInt(newv);
            }

            case "decr": {
              const key = reply[1];
              const num = parseInt(store[key]) || 0;
              const newv = num - 1;
              store[key] = String(newv);
              saveToFile();
              walLog(`DECR ${key}`);
              return sendInt(newv);
            }

            // -------------------------
            // TTL
            // -------------------------
            case "expire": {
              const [, key, seconds] = reply;
              if (!store.hasOwnProperty(key)) return sendInt(0);
              expiryTimes[key] = Date.now() + parseInt(seconds) * 1000;
              saveToFile();
              return sendInt(1);
            }

            case "ttl": {
              const key = reply[1];
              if (!expiryTimes[key]) return connection.write(":-1\r\n");
              const ttl = Math.floor((expiryTimes[key] - Date.now()) / 1000);
              return sendInt(ttl > 0 ? ttl : -2);
            }

            // -------------------------
            // TRANSACTIONS
            // -------------------------
            case "multi": {
              transactions.set(connection, []);
              return sendOK();
            }

            case "exec": {
              const queued = transactions.get(connection) || [];
              transactions.delete(connection);
              // execute queued commands
              queued.forEach((q) => {
                try {
                  // call returnReply recursively
                  const nestedParser = new Parser({
                    returnReply: (r) => parser.returnReply(r),
                    returnError: (e) => console.error("tx parser err", e),
                  });
                  nestedParser.returnReply(q);
                } catch (e) {
                  console.error("tx exec err:", e);
                }
              });
              return connection.write("+EXEC COMPLETE\r\n");
            }

            case "discard": {
              transactions.delete(connection);
              return connection.write("+DISCARDED\r\n");
            }

            // -------------------------
            // MULTI-KEY
            // -------------------------
            case "mget": {
              const keys = reply.slice(1);
              const arr = keys.map((k) =>
                store[k] === undefined
                  ? null
                  : typeof store[k] === "object"
                  ? JSON.stringify(store[k])
                  : String(store[k])
              );
              // RESP array with possible nulls ($-1)
              let out = `*${arr.length}\r\n`;
              arr.forEach((v) => {
                if (v === null) out += "$-1\r\n";
                else out += `$${Buffer.byteLength(v)}\r\n${v}\r\n`;
              });
              return connection.write(out);
            }

            case "mset": {
              // MSET key value key value ...
              if ((reply.length - 1) % 2 !== 0)
                return sendError(
                  "MSET requires even number of args after command"
                );
              for (let i = 1; i < reply.length; i += 2) {
                const key = reply[i],
                  value = reply[i + 1];
                let stored = value;
                try {
                  if (
                    typeof value === "string" &&
                    (value.trim().startsWith("{") ||
                      value.trim().startsWith("["))
                  )
                    stored = JSON.parse(value);
                } catch {}
                store[key] = stored;
                walLog(`MSET ${key}`);
                hooks.onSet(key, stored, clientID);
                recordClient(["set", key], { type: "write", key });
              }
              saveToFile();
              return sendOK();
            }

            // -------------------------
            // LISTS
            // -------------------------
            case "lpush":
            case "rpush": {
              const [, key, value] = reply;
              if (!Array.isArray(store[key])) store[key] = [];
              if (cmd === "lpush") store[key].unshift(value);
              else store[key].push(value);
              saveToFile();
              walLog(`${cmd.toUpperCase()} ${key}`);
              return sendInt(store[key].length);
            }

            case "lpushx":
            case "rpushx": {
              // Only push if key exists and is list
              const [, key, value] = reply;
              if (!store.hasOwnProperty(key) || !Array.isArray(store[key]))
                return sendInt(0);
              if (cmd === "lpushx") store[key].unshift(value);
              else store[key].push(value);
              saveToFile();
              walLog(`${cmd.toUpperCase()} ${key}`);
              return sendInt(store[key].length);
            }

            case "lrange": {
              const [, key, startRaw, stopRaw] = reply;
              const arr = Array.isArray(store[key]) ? store[key] : [];
              const start = parseInt(startRaw);
              const stop = parseInt(stopRaw);
              // handle negative indices like redis
              const s = start < 0 ? Math.max(arr.length + start, 0) : start;
              const e = stop < 0 ? arr.length + stop : stop;
              const slice = arr.slice(s, e + 1);
              // respond as array
              let out = `*${slice.length}\r\n`;
              slice.forEach(
                (i) => (out += `$${Buffer.byteLength(String(i))}\r\n${i}\r\n`)
              );
              return connection.write(out);
            }

            // -------------------------
            // HASHES
            // -------------------------
            case "hset": {
              // HSET key field value
              const [, key, field, value] = reply;
              if (typeof store[key] !== "object" || Array.isArray(store[key]))
                store[key] = {};
              const added = store[key][field] === undefined ? 1 : 0;
              store[key][field] = value;
              saveToFile();
              walLog(`HSET ${key} ${field}`);
              return sendInt(1);
            }

            case "hget": {
              const [, key, field] = reply;
              const val = store[key]?.[field];
              if (val === undefined) return connection.write("$-1\r\n");
              return connection.write(bulkString(val));
            }

            case "hgetall": {
              const key = reply[1];
              const obj = store[key] || {};
              if (typeof obj !== "object" || Array.isArray(obj))
                return connection.write("*0\r\n");
              const fields = [];
              for (const f in obj) {
                fields.push(f);
                fields.push(String(obj[f]));
              }
              // RESP array with field/value sequence
              let out = `*${fields.length}\r\n`;
              fields.forEach(
                (v) => (out += `$${Buffer.byteLength(v)}\r\n${v}\r\n`)
              );
              return connection.write(out);
            }

            case "hdel": {
              const [, key, field] = reply;
              if (store[key] && store[key][field] !== undefined) {
                delete store[key][field];
                saveToFile();
                walLog(`HDEL ${key} ${field}`);
                return sendInt(1);
              }
              return sendInt(0);
            }

            // -------------------------
            // JSON
            // -------------------------
            case "jsonset": {
              // alias JSON.SET key jsonString
              const [, key, jsonString] = reply;
              try {
                store[key] = JSON.parse(jsonString);
                saveToFile();
                walLog(`JSON.SET ${key}`);
                hooks.onSet(key, store[key], clientID);
                return sendOK();
              } catch {
                return sendError("invalid JSON");
              }
            }

            case "jsonget": {
              const key = reply[1];
              const val = store[key];
              if (val === undefined) return connection.write("$-1\r\n");
              return connection.write(bulkString(JSON.stringify(val)));
            }

            case "json.update": // JSON.UPDATE key path jsonPart (simple shallow merge)
            case "jsonupdate": {
              // We'll support: JSON.UPDATE key field jsonString  -> merges top-level field or sets path
              const [, key, pathStr, jsonPart] = reply;
              if (!store[key] || typeof store[key] !== "object")
                store[key] = {};
              try {
                const patch = JSON.parse(jsonPart);
                // If path is "." or "$" or empty, merge into root
                const p = (pathStr || "").replace(/^\$\.?/, ""); // naive
                if (!p || p === "." || p === "$") {
                  // shallow merge
                  Object.assign(store[key], patch);
                } else {
                  // set nested key (dot separated)
                  const parts = p.split(".");
                  let cur = store[key];
                  for (let i = 0; i < parts.length - 1; i++) {
                    if (!cur[parts[i]] || typeof cur[parts[i]] !== "object")
                      cur[parts[i]] = {};
                    cur = cur[parts[i]];
                  }
                  cur[parts[parts.length - 1]] = Object.assign(
                    {},
                    cur[parts[parts.length - 1]] || {},
                    patch
                  );
                }
                saveToFile();
                walLog(`JSON.UPDATE ${key} ${pathStr}`);
                return sendOK();
              } catch (e) {
                return sendError("invalid JSON patch");
              }
            }

            // -------------------------
            // HTTP/HTTPS FETCH WITH CACHE
            // -------------------------
            case "fetch": {
              const url = reply[1];
              if (
                fetchCache[url] &&
                (!fetchCache[url].expiresAt ||
                  Date.now() < fetchCache[url].expiresAt)
              ) {
                const data = fetchCache[url].data;
                return connection.write(bulkString(data));
              }
              const client = url.startsWith("https") ? https : http;
              client
                .get(url, (res) => {
                  let data = "";
                  res.on("data", (chunk) => (data += chunk));
                  res.on("end", () => {
                    fetchCache[url] = { data, expiresAt: null }; // no expiry by default
                    saveToFile();
                    connection.write(bulkString(data));
                  });
                })
                .on("error", (err) =>
                  connection.write(`-ERR ${err.message}\r\n`)
                );
              return;
            }

            case "fetchcache": {
              // FETCHCACHE url expire_seconds
              const [, url, sec] = reply;
              const expireSeconds = parseInt(sec) || 86400; // default 24h
              if (
                fetchCache[url] &&
                Date.now() < (fetchCache[url].expiresAt || 0)
              ) {
                return connection.write(bulkString(fetchCache[url].data));
              }
              const client = url.startsWith("https") ? https : http;
              client
                .get(url, (res) => {
                  let data = "";
                  res.on("data", (chunk) => (data += chunk));
                  res.on("end", () => {
                    fetchCache[url] = {
                      data,
                      expiresAt: Date.now() + expireSeconds * 1000,
                    };
                    saveToFile();
                    return connection.write(bulkString(data));
                  });
                })
                .on("error", (err) =>
                  connection.write(`-ERR ${err.message}\r\n`)
                );
              return;
            }

            // -------------------------
            // PUB/SUB
            // -------------------------
            case "subscribe": {
              const channel = reply[1];
              if (!subscribers.has(channel))
                subscribers.set(channel, new Set());
              subscribers.get(channel).add(clientID);
              clients.get(clientID).subscribedChannels.add(channel);
              recordClient(["subscribe", channel], {
                type: "subscribe",
                channel,
              });
              connection.write(`+Subscribed to ${channel}\r\n`);
              break;
            }

            case "unsubscribe": {
              const channel = reply[1];
              if (subscribers.has(channel)) {
                subscribers.get(channel).delete(clientID);
                clients.get(clientID).subscribedChannels.delete(channel);
              }
              connection.write(`+Unsubscribed from ${channel}\r\n`);
              break;
            }

            case "publish": {
              const [, channel, message] = reply;
              const subs = subscribers.get(channel) || new Set();
              let count = 0;
              subs.forEach((cid) => {
                const cmeta = clients.get(cid);
                if (cmeta && cmeta.connection && !cmeta.connection.destroyed) {
                  cmeta.connection.write(`+MESSAGE ${channel} ${message}\r\n`);
                  count++;
                }
              });
              hooks.onPublish(channel, message, clientID);
              walLog(`PUBLISH ${channel}`);
              recordClient(["publish", channel, message], {
                type: "publish",
                channel,
                message,
              });
              sendInt(count);
              break;
            }

            // -------------------------
            // ADMIN / INFO / CLIENT
            // -------------------------
            case "info": {
              const info = `# Server
Custom Redis Node Server
# Clients
Connected Clients: ${clients.size}
# Keys
Keys: ${Object.keys(store).length}
# Replica
ReplicaOf: ${replicaOf ? `${replicaOf.host}:${replicaOf.port}` : "none"}
# Uptime
${process.uptime()} seconds
`;
              return connection.write(bulkString(info));
            }

            case "client": {
              // CLIENT LIST | CLIENT INFO <id> | CLIENT KILL <id>
              const subcmd = String(reply[1] || "").toLowerCase();
              if (subcmd === "list") {
                // return array of lines like "id=1 addr=... cmds=..."
                const lines = [];
                for (const [id, meta] of clients.entries()) {
                  lines.push(
                    `id=${id} addr=${
                      meta.remoteAddress || "unknown"
                    } connectedAt=${new Date(
                      meta.connectedAt
                    ).toISOString()} cmds=${meta.commandsExecuted}`
                  );
                }
                return connection.write(arrayOfBulk(lines));
              } else if (subcmd === "info") {
                const id = parseInt(reply[2]);
                const meta = clients.get(id);
                if (!meta) return sendError("no such client");
                const info = `id:${meta.id}\naddr:${
                  meta.remoteAddress
                }\nconnectedAt:${new Date(
                  meta.connectedAt
                ).toISOString()}\ncommandsExecuted:${
                  meta.commandsExecuted
                }\nsubscribed:${Array.from(meta.subscribedChannels).join(",")}`;
                return connection.write(bulkString(info));
              } else if (subcmd === "kill") {
                const id = parseInt(reply[2]);
                const meta = clients.get(id);
                if (!meta) return sendError("no such client");
                meta.connection.end(`+KILLED by ${clientID}\r\n`);
                // cleanup
                for (const ch of meta.subscribedChannels) {
                  subscribers.get(ch)?.delete(id);
                }
                clients.delete(id);
                clientRecords.delete(id);
                return sendOK();
              } else {
                return sendError("Unknown CLIENT subcommand");
              }
            }

            case "metrics": {
              const metrics = `totalCommands:${totalCommands}\nconnectedClients:${
                clients.size
              }\nkeys:${Object.keys(store).length}\n`;
              return connection.write(bulkString(metrics));
            }

            case "keys": {
              // KEYS pattern (basic * wildcard)
              const pattern = reply[1] || "*";
              // Transform simple glob to regex
              const regex = new RegExp(
                "^" + pattern.split("*").map(escapeRegex).join(".*") + "$"
              );
              const keys = Object.keys(store).filter((k) => regex.test(k));
              return connection.write(arrayOfBulk(keys));
            }

            case "flushall": {
              store = {};
              expiryTimes = {};
              fetchCache = {};
              saveToFile();
              walLog("FLUSHALL");
              return sendOK();
            }

            case "save": {
              saveToFile();
              return sendOK();
            }

            case "replicaof": {
              // REPLICAOF host port
              const [, host, port] = reply;
              if (!host || !port) {
                replicaOf = null;
                return connection.write("+REPLICA OFF\r\n");
              }
              replicaOf = { host, port: parseInt(port) };
              return connection.write(`+REPLICA ${host}:${port}\r\n`);
            }

            default:
              return connection.write(`-ERR unknown command '${cmd}'\r\n`);
          } // end of switch
        } catch (err) {
          console.error("Command error:", err);
          return connection.write(`-ERR ${err.message}\r\n`);
        }
      }, // returnReply
      returnError: (err) => {
        console.error("Parser error:", err);
        connection.write(`-ERR parser ${err.message}\r\n`);
      },
    });

    try {
      parser.execute(data);
    } catch (e) {
      console.error("Parser execute failed:", e);
      connection.write(`-ERR parser_execute ${e.message}\r\n`);
    }
  }); // end connection.on data

  connection.on("end", () => {
    // cleanup client subscriptions
    const meta = clients.get(clientID);
    if (meta) {
      for (const ch of meta.subscribedChannels) {
        const s = subscribers.get(ch);
        if (s) s.delete(clientID);
      }
    }
    clients.delete(clientID);
    console.log(`🔴 Client ${clientID} disconnected`);
  });

  connection.on("error", (err) => {
    console.log(`Client ${clientID} connection error:`, err.message);
    clients.delete(clientID);
  });
}); // end server

function escapeRegex(s) {
  return String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// Start listening
const PORT = process.env.PORT ? parseInt(process.env.PORT) : 6379;
server.listen(PORT, () =>
  console.log(`Custom Redis Server running on port ${PORT}`)
);
