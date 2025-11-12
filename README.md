# 🧠 Custom Redis

## ℹ️ Overview

**Custom Redis** is a simplified, educational implementation of an **in-memory key-value store**, inspired by [Redis](https://redis.io/), and built using **Node.js**.  
It supports basic command handling over TCP and optionally persists data in a JSON file.

This project is ideal for understanding how Redis-like databases handle **connections**, **commands**, and **data persistence** at a low level.

---

## ⚙️ Features

- 🗝️ **Key-Value Storage:**  
  Supports basic commands such as `SET` and `GET`.

- 🧩 **Node.js TCP Server:**  
  Built using the native `net` module to manage client connections.

- 🧠 **Command Parsing Logic:**  
  Core logic for handling commands resides in `basic_redis.js`.

- 💾 **JSON Persistence:**  
  Uses `redis_data.json` for saving and loading stored data between sessions.

- 🧑‍💻 **Lightweight and Easy to Understand:**  
  Minimal dependencies — perfect for learning or experimenting with backend systems.

---

## 📂 Project Structure

```

.
├── index.js          # Main server entry point (creates TCP server, listens on a port)
├── basic_redis.js    # Core logic for handling Redis-like commands
├── redis_data.json   # JSON file for persisting key-value data
├── package.json      # Project metadata and dependencies
├── package-lock.json
└── node_modules/

````

---

## 🚀 Getting Started

### 1️⃣ Install Dependencies

Make sure you have **Node.js** installed.  
Then, install all required dependencies using:

```bash
npm install
````

---

### 2️⃣ Run the Server

Start the TCP server using:

```bash
node index.js
```

By default, it listens on a predefined port (e.g., `6379` or `5000`, depending on configuration).

---

### 3️⃣ Connect to the Server

You can connect using any TCP client like **Telnet** or **Netcat**:

#### Using Telnet:

```bash
telnet localhost [PORT]
```

#### Using Netcat:

```bash
nc localhost [PORT]
```

Once connected, try basic commands:

```bash
SET mykey "Hello World"
GET mykey
```

---

## 🧰 Example Interaction

```
> SET language "Node.js"
OK
> GET language
"Node.js"
```

---

## 💡 Notes

* This is a **learning-oriented project**, not a production-ready database.
* Data persistence relies on `redis_data.json`; if deleted, data will be lost.
* Extend it by adding more commands like:

  * `DEL key` → Delete a key
  * `EXISTS key` → Check if a key exists
  * `FLUSHALL` → Clear all data

---

## 📜 License

This project is licensed under the **MIT License** – free to use and modify.
