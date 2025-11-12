const net = require("net");
const fs = require("fs");
const path = require("path");
const Parser = require("redis-parser");

const DATA_FILE = path.join(__dirname, "redis_data.json");

let store = {};
let expiryTimes = {};

// Load data from file on startup, if it exists
if (fs.existsSync(DATA_FILE)) {
  try {
    const fileData = JSON.parse(fs.readFileSync(DATA_FILE, "utf8"));
    store = fileData.store || {};
    expiryTimes = fileData.expiryTimes || {};
    console.log("Data loaded from file.");
  } catch (err) {
    console.error("Error loading data file:", err);
  }
}

// Function to save the current state to the JSON file
function saveToFile() {
  try {
    fs.writeFileSync(DATA_FILE, JSON.stringify({ store, expiryTimes }, null, 2));
  } catch (err) {
    console.error("Error saving data to file:", err);
  }
}

// Function to check for and remove expired keys
function checkExpiry() {
  const now = Date.now();
  let changed = false;
  for (const key in expiryTimes) {
    if (now > expiryTimes[key]) {
      delete store[key];
      delete expiryTimes[key];
      changed = true;
    }
  }
  // Only save to file if something was actually deleted
  if (changed) {
    saveToFile();
  }
}

// Run expiry check every 5 seconds
setInterval(checkExpiry, 5000);

const server = net.createServer((connection) => {
  console.log("Client connected");

  connection.on("data", (data) => {
    const parser = new Parser({
      returnReply: (reply) => {
        const command = reply[0].toLowerCase();

        switch (command) {
          // SET key value
          case "set": {
            const key = reply[1];
            let value = reply[2];

            // Attempt to parse JSON strings
            if (
              typeof value === "string" &&
              (value.trim().startsWith("{") || value.trim().startsWith("["))
            ) {
              try {
                value = JSON.parse(value);
              } catch (e) {
                // Not valid JSON, treat as string
              }
            }

            store[key] = value;
            delete expiryTimes[key]; // SET removes any existing expiry
            saveToFile();
            connection.write("+OK\r\n");
            break;
          }

          // SETEX key seconds value
          case "setex": {
            const key = reply[1];
            const seconds = parseInt(reply[2]);
            let value = reply[3];

            if (isNaN(seconds) || seconds <= 0) {
              connection.write("-ERR invalid expire time in setex\r\n");
              break;
            }

            if (
              typeof value === "string" &&
              (value.trim().startsWith("{") || value.trim().startsWith("["))
            ) {
              try {
                value = JSON.parse(value);
              } catch (e) {
                // Not valid JSON, treat as string
              }
            }

            store[key] = value;
            expiryTimes[key] = Date.now() + seconds * 1000;
            saveToFile();
            connection.write("+OK\r\n");
            break;
          }

          // NEW: EXPIRE key seconds
          case "expire": {
            const key = reply[1];
            const seconds = parseInt(reply[2]);

            if (isNaN(seconds) || seconds <= 0) {
              connection.write("-ERR invalid expire time in expire\r\n");
              break;
            }

            if (store[key] === undefined) {
              connection.write(":0\r\n"); // Key does not exist
            } else {
              expiryTimes[key] = Date.now() + seconds * 1000;
              saveToFile();
              connection.write(":1\r\n"); // Expiry set
            }
            break;
          }

          // NEW: TTL key
          case "ttl": {
            const key = reply[1];

            if (store[key] === undefined) {
              connection.write(":-2\r\n"); // Key does not exist
              break;
            }

            if (expiryTimes[key] === undefined) {
              connection.write(":-1\r\n"); // Key exists but has no expiry
              break;
            }

            const now = Date.now();
            if (expiryTimes[key] < now) {
                // Key is expired, delete it
                delete store[key];
                delete expiryTimes[key];
                saveToFile();
                connection.write(":-2\r\n"); // Key does not exist (as it was expired)
            } else {
                // Key exists and has expiry
                const remainingSeconds = Math.round((expiryTimes[key] - now) / 1000);
                connection.write(`:${remainingSeconds}\r\n`);
            }
            break;
          }

          // GET key
          case "get": {
            const key = reply[1];

            // Check for expiry on GET
            if (expiryTimes[key] && Date.now() > expiryTimes[key]) {
              delete store[key];
              delete expiryTimes[key];
              saveToFile();
            }

            const value = store[key];
            if (value === undefined) {
              connection.write("$-1\r\n"); // Nil reply
            } else {
              let output;

              // Stringify objects for transmission
              if (typeof value === "object") {
                output = JSON.stringify(value); // More compact than with newlines
              } else {
                output = String(value);
              }
              const byteLength = Buffer.byteLength(output, "utf8");
              connection.write(`$${byteLength}\r\n${output}\r\n`);
            }
            break;
          }

          // DEL key
          case "del": {
            const key = reply[1];
            const deleted = store.hasOwnProperty(key); // Check if key exists before delete
            delete store[key];
            delete expiryTimes[key];
            if (deleted) {
              saveToFile();
            }
            connection.write(`:${deleted ? 1 : 0}\r\n`);
            break;
          }

          // KEYS
          case "keys": {
            const keys = Object.keys(store);
            const response =
              `*${keys.length}\r\n` +
              keys.map((k) => {
                const byteLength = Buffer.byteLength(k, "utf8");
                return `$${byteLength}\r\n${k}\r\n`
              }).join("");
            connection.write(response);
            break;
          }

          // FLUSHALL
          case "flushall": {
            store = {};
            expiryTimes = {};
            saveToFile();
            connection.write("+OK\r\n");
            break;
          }

          default:
            connection.write(`-ERR unknown command '${command}'\r\n`);
        }
      },

      returnError: (err) => {
        console.error("Parser Error:", err);
        connection.write("-ERR Protocol error\r\n");
      },
    });

    parser.execute(data);
  });

  connection.on("end", () => console.log("Client disconnected"));
  connection.on("error", (err) => console.error("Connection error:", err));
});

server.listen(6379, () =>
  console.log("Custom Redis Server running on port 6379")
);
