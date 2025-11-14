const net = require("net");
const Parser = require("redis-parser");

const store = {};
const server = net.createServer((connection) => {
  console.log("Client connected");

  connection.on("data", (data) => {
    const parser = new Parser({
      returnReply: (reply) => {
        const command = reply[0];
        switch (command) {
          case "set":
            {
              const key = reply[1];
              const value = reply[2];
              store[key] = value;
              connection.write("+OK\r\n");
            }
            break;
          case "get": {
            const key = reply[1];
            const value = store[key];
            if (!value) connection.write("$-1\r\n");
            else connection.write(`$${value.length}\r\n${value}\r\n`);
          }
        }
      },
      returnError: (err) => {
        console.log("=>", err);
      },
    });
    parser.execute(data);
  });
});

server.listen(6379, () =>
  console.log(`Custom Redis Server running on port 6379`)
);

// const net = require("net");
// const fs = require("fs");
// const path = require("path");
// const Parser = require("redis-parser");

// const DATA_FILE = path.join(__dirname, "redis_data.json");

// let store = {};
// let expiryTimes = {};

// if (fs.existsSync(DATA_FILE)) {
//   try {
//     const fileData = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
//     store = fileData.store || {};
//     expiryTimes = fileData.expiryTimes || {};
//     console.log("Data loaded from file.");
//   } catch (err) {
//     console.error("Error loading data file:", err);
//   }
// }

// function saveToFile() {
//   fs.writeFileSync(DATA_FILE, JSON.stringify({ store, expiryTimes }, null, 2));
// }

// function checkExpiry() {
//   const now = Date.now();
//   for (const key in expiryTimes) {
//     if (now > expiryTimes[key]) {
//       delete store[key];
//       delete expiryTimes[key];
//     }
//   }
//   saveToFile();
// }

// // Run expiry check every 5 seconds
// setInterval(checkExpiry, 5000);

// const server = net.createServer((connection) => {
//   console.log("Client connected");

//   connection.on("data", (data) => {
//     const parser = new Parser({
//       returnReply: (reply) => {
//         const command = reply[0].toLowerCase();

//         switch (command) {
//           // SET key value
//           case "set": {
//             const key = reply[1];
//             let value = reply[2];

//             if (
//               typeof value === "string" &&
//               (value.trim().startsWith("{") || value.trim().startsWith("["))
//             ) {
//               try {
//                 value = JSON.parse(value);
//               } catch (e) {}
//             }

//             store[key] = value;
//             delete expiryTimes[key];
//             saveToFile();
//             connection.write("+OK\r\n");
//             break;
//           }

//           case "setex": {
//             const key = reply[1];
//             const seconds = parseInt(reply[2]);
//             let value = reply[3];

//             if (
//               typeof value === "string" &&
//               (value.trim().startsWith("{") || value.trim().startsWith("["))
//             ) {
//               try {
//                 value = JSON.parse(value);
//               } catch (e) {}
//             }

//             store[key] = value;
//             expiryTimes[key] = Date.now() + seconds * 1000;
//             saveToFile();
//             connection.write("+OK\r\n");
//             break;
//           }

//           // GET key
//           case "get": {
//             const key = reply[1];

//             if (expiryTimes[key] && Date.now() > expiryTimes[key]) {
//               delete store[key];
//               delete expiryTimes[key];
//               saveToFile();
//             }

//             const value = store[key];
//             if (value === undefined) {
//               connection.write("$-1\r\n");
//             } else {
//               let output;

//               if (typeof value === "object") {
//                 output = JSON.stringify(value, null, 2);
//               } else {
//                 output = String(value);
//               }
//               const byteLength = Buffer.byteLength(output, "utf8");
//               connection.write(`$${byteLength}\r\n${output}\r\n`);
//             }
//             break;
//           }

//           // DEL key
//           case "del": {
//             const key = reply[1];
//             const deleted = delete store[key];
//             delete expiryTimes[key];
//             saveToFile();
//             connection.write(`:${deleted ? 1 : 0}\r\n`);
//             break;
//           }

//           // KEYS
//           case "keys": {
//             const keys = Object.keys(store);
//             const response =
//               `*${keys.length}\r\n` +
//               keys.map((k) => `$${k.length}\r\n${k}\r\n`).join("");
//             connection.write(response);
//             break;
//           }

//           // FLUSHALL
//           case "flushall": {
//             store = {};
//             expiryTimes = {};
//             saveToFile();
//             connection.write("+OK\r\n");
//             break;
//           }

//           default:
//             connection.write(`-ERR unknown command '${command}'\r\n`);
//         }
//       },

//       returnError: (err) => {
//         console.error(" Parser Error:", err);
//       },
//     });

//     parser.execute(data);
//   });

//   connection.on("end", () => console.log("Client disconnected"));
// });

// server.listen(6379, () =>
//   console.log(" Custom Redis Server running on port 6379")
// );


// /**
//  *  Advanced Custom Redis Server using Node.js
//  * Features:
//  * - Persistent JSON Storage
//  * - Transactions (MULTI/EXEC/DISCARD)
//  * - Lists, Hashes, JSON Objects
//  * - TTL and Expiry
//  * - HTTPS/HTTP Fetch with caching
//  * - Simulated Database Connections
//  * - Atomic Increment/Decrement
//  * - Publish/Subscribe System
//  * - Persistent Write-Ahead Logging (WAL)
//  */

// const net = require("net");
// const fs = require("fs");
// const path = require("path");
// const Parser = require("redis-parser");
// const http = require("http");
// const https = require("https");

// const DATA_FILE = path.join(__dirname, "redis_data.json");
// const LOG_FILE = path.join(__dirname, "redis_wal.log");

// let store = {};
// let expiryTimes = {};
// let subscribers = new Map(); // pub/sub channels
// let transactions = new Map();

// // Load persisted data
// if (fs.existsSync(DATA_FILE)) {
//   try {
//     const fileData = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
//     store = fileData.store || {};
//     expiryTimes = fileData.expiryTimes || {};
//     console.log("✅ Data loaded from disk.");
//   } catch (err) {
//     console.error("❌ Error loading data:", err);
//   }
// }

// function saveToFile() {
//   fs.writeFileSync(DATA_FILE, JSON.stringify({ store, expiryTimes }, null, 2));
// }

// function logCommand(command) {
//   fs.appendFileSync(LOG_FILE, command + "\n");
// }

// function checkExpiry() {
//   const now = Date.now();
//   for (const key in expiryTimes) {
//     if (now > expiryTimes[key]) {
//       delete store[key];
//       delete expiryTimes[key];
//     }
//   }
//   saveToFile();
// }

// setInterval(checkExpiry, 5000);

// const server = net.createServer((connection) => {
//   console.log("🟢 Client connected");

//   connection.on("data", (data) => {
//     const parser = new Parser({
//       returnReply: (reply) => {
//         const command = reply[0].toLowerCase();

//         // Handle transactions
//         if (
//           transactions.has(connection) &&
//           !["exec", "discard", "multi"].includes(command)
//         ) {
//           const queued = transactions.get(connection);
//           queued.push(reply);
//           connection.write("+QUEUED\r\n");
//           return;
//         }

//         switch (command) {
//           // ---------- BASIC COMMANDS ---------- //
//           case "set": {
//             const [_, key, value] = reply;
//             store[key] = value;
//             delete expiryTimes[key];
//             saveToFile();
//             logCommand(reply.join(" "));
//             connection.write("+OK\r\n");
//             break;
//           }

//           case "get": {
//             const key = reply[1];
//             const value = store[key];
//             if (value === undefined) return connection.write("$-1\r\n");
//             const str =
//               typeof value === "object" ? JSON.stringify(value) : String(value);
//             connection.write(`$${Buffer.byteLength(str)}\r\n${str}\r\n`);
//             break;
//           }

//           case "del": {
//             const key = reply[1];
//             const deleted = delete store[key];
//             delete expiryTimes[key];
//             saveToFile();
//             connection.write(`:${deleted ? 1 : 0}\r\n`);
//             break;
//           }

//           case "exists": {
//             const key = reply[1];
//             connection.write(`:${store.hasOwnProperty(key) ? 1 : 0}\r\n`);
//             break;
//           }

//           case "incr": {
//             const key = reply[1];
//             store[key] = (parseInt(store[key]) || 0) + 1;
//             saveToFile();
//             connection.write(`:${store[key]}\r\n`);
//             break;
//           }

//           case "decr": {
//             const key = reply[1];
//             store[key] = (parseInt(store[key]) || 0) - 1;
//             saveToFile();
//             connection.write(`:${store[key]}\r\n`);
//             break;
//           }

//           // ---------- TTL & EXPIRY ---------- //
//           case "expire": {
//             const [_, key, seconds] = reply;
//             if (!store[key]) return connection.write(":0\r\n");
//             expiryTimes[key] = Date.now() + parseInt(seconds) * 1000;
//             saveToFile();
//             connection.write(":1\r\n");
//             break;
//           }

//           case "ttl": {
//             const key = reply[1];
//             if (!expiryTimes[key]) return connection.write(":-1\r\n");
//             const ttl = Math.floor((expiryTimes[key] - Date.now()) / 1000);
//             connection.write(`:${ttl > 0 ? ttl : -2}\r\n`);
//             break;
//           }

//           // ---------- TRANSACTIONS ---------- //
//           case "multi":
//             transactions.set(connection, []);
//             connection.write("+OK\r\n");
//             break;

//           case "exec":
//             const queued = transactions.get(connection) || [];
//             transactions.delete(connection);
//             for (const cmd of queued) parser.returnReply(cmd);
//             connection.write("+EXEC COMPLETE\r\n");
//             break;

//           case "discard":
//             transactions.delete(connection);
//             connection.write("+DISCARDED\r\n");
//             break;

//           // ---------- LIST COMMANDS ---------- //
//           case "lpush":
//           case "rpush": {
//             const [_, key, value] = reply;
//             if (!Array.isArray(store[key])) store[key] = [];
//             command === "lpush"
//               ? store[key].unshift(value)
//               : store[key].push(value);
//             saveToFile();
//             connection.write(`:${store[key].length}\r\n`);
//             break;
//           }

//           case "lrange": {
//             const [_, key, start, stop] = reply;
//             const arr = store[key] || [];
//             const slice = arr.slice(parseInt(start), parseInt(stop) + 1);
//             const resp =
//               `*${slice.length}\r\n` +
//               slice.map((i) => `$${i.length}\r\n${i}\r\n`).join("");
//             connection.write(resp);
//             break;
//           }

//           // ---------- HASH COMMANDS ---------- //
//           case "hset": {
//             const [_, key, field, value] = reply;
//             if (typeof store[key] !== "object" || Array.isArray(store[key]))
//               store[key] = {};
//             store[key][field] = value;
//             saveToFile();
//             connection.write(":1\r\n");
//             break;
//           }

//           case "hget": {
//             const [_, key, field] = reply;
//             const val = store[key]?.[field];
//             if (val === undefined) return connection.write("$-1\r\n");
//             connection.write(`$${val.length}\r\n${val}\r\n`);
//             break;
//           }

//           // ---------- JSON / OBJECT ---------- //
//           case "jsonset": {
//             const [_, key, jsonString] = reply;
//             try {
//               store[key] = JSON.parse(jsonString);
//               saveToFile();
//               connection.write("+OK\r\n");
//             } catch {
//               connection.write("-ERR invalid JSON\r\n");
//             }
//             break;
//           }

//           case "jsonget": {
//             const key = reply[1];
//             const value = store[key];
//             if (!value) return connection.write("$-1\r\n");
//             const str = JSON.stringify(value, null, 2);
//             connection.write(`$${Buffer.byteLength(str)}\r\n${str}\r\n`);
//             break;
//           }

//           // ---------- HTTP/HTTPS FETCH WITH CACHING ---------- //
//           case "fetch": {
//             const url = reply[1];
//             if (store[url]) {
//               connection.write(`$${store[url].length}\r\n${store[url]}\r\n`);
//               return;
//             }

//             const client = url.startsWith("https") ? https : http;
//             client
//               .get(url, (res) => {
//                 let data = "";
//                 res.on("data", (chunk) => (data += chunk));
//                 res.on("end", () => {
//                   store[url] = data;
//                   saveToFile();
//                   connection.write(`$${data.length}\r\n${data}\r\n`);
//                 });
//               })
//               .on("error", (err) => {
//                 connection.write(`-ERR ${err.message}\r\n`);
//               });
//             break;
//           }

//           // ---------- PUB/SUB ---------- //
//           case "subscribe": {
//             const channel = reply[1];
//             if (!subscribers.has(channel)) subscribers.set(channel, []);
//             subscribers.get(channel).push(connection);
//             connection.write(`+Subscribed to ${channel}\r\n`);
//             break;
//           }

//           case "publish": {
//             const [_, channel, message] = reply;
//             const subs = subscribers.get(channel) || [];
//             subs.forEach((conn) =>
//               conn.write(`+Message on ${channel}: ${message}\r\n`)
//             );
//             connection.write(`:${subs.length}\r\n`);
//             break;
//           }

//           // ---------- SIMULATED DATABASE CONNECTION ---------- //
//           case "dbconnect": {
//             const dbname = reply[1];
//             store["__current_db"] = dbname;
//             connection.write(`+Connected to simulated DB '${dbname}'\r\n`);
//             break;
//           }

//           // ---------- ADMIN COMMANDS ---------- //
//           case "info": {
//             const info = `
// # Server
// Custom Redis Node Server
// # Memory
// Keys: ${Object.keys(store).length}
// # Persistence
// Data File: ${DATA_FILE}
// # Uptime
// ${process.uptime()} seconds
// `;
//             connection.write(`$${Buffer.byteLength(info)}\r\n${info}\r\n`);
//             break;
//           }

//           default:
//             connection.write(`-ERR unknown command '${command}'\r\n`);
//         }
//       },
//       returnError: (err) => console.error("Parser Error:", err),
//     });

//     parser.execute(data);
//   });

//   connection.on("end", () => console.log("🔴 Client disconnected"));
// });

// server.listen(6379, () =>
//   console.log(" Custom Redis Server running on port 6379")
// );


/**
 * advanced_redis.js
 * Enhanced Redis-like server (Node.js, net module)
 *
 * Features:
 * - Client ID tracking + per-client records
 * - Extended commands: MGET, MSET, LPUSHX, RPUSHX, HGETALL, HDEL, KEYS, FLUSHALL, etc.
 * - Pub/Sub with per-client subscription tracking and publish history
 * - FETCHCACHE with expiry
 * - JSON.SET/GET/UPDATE
 * - TTL support for keys and cache entries
 * - WAL logging + snapshot save
 * - CLIENT commands: LIST, INFO, KILL
 * - REPLICAOF (simulation)
 */