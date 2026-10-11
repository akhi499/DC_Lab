# DFS Lab

This folder contains the distributed file system experiment with chunking, replication, failover, and repair.

## Included files

- [proto/dfs.proto](proto/dfs.proto)
- [dfs_metadata.py](dfs_metadata.py)
- [dfs_storage.py](dfs_storage.py)
- [dfs_client.py](dfs_client.py)
- [dfs_repair.py](dfs_repair.py)
- [net_config.py](net_config.py)
- [docs/dfs_experiment_11.md](docs/dfs_experiment_11.md)
- [screenshots](screenshots)

## Run from this folder

```powershell
cd dfs_lab
.\.venv\Scripts\python.exe dfs_metadata.py
```

Then start the storage nodes and client in separate terminals from the same folder.
