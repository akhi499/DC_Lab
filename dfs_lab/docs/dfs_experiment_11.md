# Experiment 11: Distributed File System Storage

This note captures the design, setup flow, and verification steps for the chunked, replicated file-system lab.

## 1. Architecture overview

The solution has three process types:

- Metadata server: keeps file metadata, chunk placement, Lamport versions, and node liveness.
- Storage nodes: hold chunk data on disk and send heartbeats.
- Client: writes a file in chunks, reads by checksum, and triggers repair when replicas are missing.

![Architecture](../screenshots/01_cluster_architecture.png)

## 2. Service contract

The contract is defined in [../proto/dfs.proto](../proto/dfs.proto). It exposes:

- `DfsMetadataService` for heartbeat, file allocation, commit, metadata lookup, status, and replica repair
- `DfsStorageService` for `PutChunk` and `GetChunk`

Generate the stubs from inside the lab folder:

```powershell
cd dfs_lab
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I=proto --python_out=. --grpc_python_out=. proto/dfs.proto
```

## 3. Run the metadata service

From the lab folder, start the metadata process:

```powershell
cd dfs_lab
.\.venv\Scripts\python.exe dfs_metadata.py
```

This service keeps state in `dfs_data/metadata.json` and updates the Lamport clock for file versions.

## 4. Run the storage nodes

Open three terminals in the lab folder and run:

```powershell
cd dfs_lab
.\.venv\Scripts\python.exe dfs_storage.py 1
.\.venv\Scripts\python.exe dfs_storage.py 2
.\.venv\Scripts\python.exe dfs_storage.py 3
```

The storage nodes heartbeats keep the metadata service aware of which replicas are alive.

## 5. Upload a file

Create a test file and upload it from the lab folder:

```powershell
cd dfs_lab
.\.venv\Scripts\python.exe -c "import os, pathlib; pathlib.Path('photo.bin').write_bytes(os.urandom(300000))"
.\.venv\Scripts\python.exe dfs_client.py status
.\.venv\Scripts\python.exe dfs_client.py put photo.bin
```

A successful upload writes each chunk to two live nodes, then commits the metadata.

![Status after upload](../screenshots/02_status_after_upload.png)

## 6. Download and verify

```powershellcd dfs_lab.\.venv\Scripts\python.exe dfs_client.py get photo.bin out1.bin
.\.venv\Scripts\python.exe -c "import hashlib, pathlib; a=hashlib.sha256(pathlib.Path('photo.bin').read_bytes()).hexdigest(); b=hashlib.sha256(pathlib.Path('out1.bin').read_bytes()).hexdigest(); print('MATCH' if a==b else 'DIFFERENT')"
```

The client tries each replica in order and checks the checksum before accepting the data.

## 7. Simulate a node failure

Stop storage node 2 and run a read before the heartbeat timeout expires:

```powershellcd dfs_lab.\.venv\Scripts\python.exe dfs_client.py get photo.bin out2.bin
```

The client should fall back to the remaining healthy replica and still verify the full-file checksum.

## 8. Repair under-replicated chunks

Wait until the metadata service marks the node as down, then repair the missing replicas:

```powershellcd dfs_lab.\.venv\Scripts\python.exe dfs_repair.py
.\.venv\Scripts\python.exe dfs_client.py status
```

![Repair and failover](../screenshots/03_repair_and_failover.png)

This step should restore each under-replicated chunk to the configured replication factor.

## 9. Corrupt a chunk and recover

A worked test is to overwrite one replica on a live node with junk and then re-read the file:

```powershellcd dfs_lab.\.venv\Scripts\python.exe -c "from pathlib import Path; p = next(Path('dfs_data').joinpath('store-1').glob('*.c0')); p.write_bytes(b'corrupted')"
.\.venv\Scripts\python.exe dfs_client.py get photo.bin out4.bin
```

The client notices the checksum mismatch and skips the bad replica. It then reads from the healthy copy and verifies the final file checksum.

## 10. Observations and limitations

What to check in the experiment:

- every chunk is stored on two nodes
- reads continue when a node is down
- corrupted replicas are detected and skipped
- repair restores missing copies
- a re-started node rejoins with its local data intact

Known limitations:

- the metadata server is still a single point of failure
- repair only fills missing replicas; it does not scan for silent corrupt copies
- old file versions remain on disk until cleanup is added
- chunk writes are serialized by the client rather than chained across storage nodes

## 11. Practical notes

- Run every process from the same repository root so that file paths under `dfs_data/` stay consistent.
- Keep chunk sizes small enough to avoid gRPC message-size problems.
- If the metadata service is restarted, it reloads persisted state from `dfs_data/metadata.json`.
- The code in this repository is intentionally small and educational rather than production-grade.
