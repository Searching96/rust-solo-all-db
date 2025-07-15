# Data Directory

This directory contains all database files and runtime data for the Rust Solo All-DB project.

## 📁 Directory Structure

```
data/
├── examples/           # Example SSTable files for inspection
│   ├── example_output.sst
│   └── inspect_me.sst
├── runtime/           # Active database files
│   ├── sstable_*.sst     # SSTable files
│   └── wal.log           # Write-Ahead Log
└── README.md          # This file
```

## 🎯 Directory Purpose

### **examples/** - Example SSTable Files
- **example_output.sst** - Sample SSTable file for format inspection
- **inspect_me.sst** - Example SSTable for testing and debugging
- **Purpose**: Educational examples and format validation

### **runtime/** - Active Database Files
- **sstable_*.sst** - Active SSTable files created during operation
- **wal.log** - Write-Ahead Log for crash recovery
- **Purpose**: Live database operation and persistence

## 🔧 File Types

### **SSTable Files (.sst)**
- **Binary format** containing sorted key-value pairs
- **Immutable** once written
- **Compacted** periodically to optimize storage
- **Used for**: Persistent storage of database records

### **WAL Files (.log)**
- **Write-Ahead Log** for crash recovery
- **Sequential writes** for performance
- **Replayed on startup** to recover uncommitted data
- **Used for**: Durability and crash recovery

## 🚀 Usage

### **Development**
```bash
# Database files are created automatically in runtime/
cargo run --bin rustdb

# Files created during operation:
# - sstable_000000.sst, sstable_000001.sst, etc.
# - wal.log
```

### **Inspection**
```bash
# Example SSTable files can be inspected for debugging
ls data/examples/
```

### **Cleanup**
```bash
# Remove runtime data (be careful!)
rm -rf data/runtime/*.sst data/runtime/*.log

# Remove example files
rm -rf data/examples/
```

## 📊 File Management

### **Automatic Management**
- **SSTable creation**: Automatic when memtable flushes
- **WAL rotation**: Automatic when size limits reached
- **Compaction**: Automatic background process
- **Cleanup**: Old files removed after compaction

### **Manual Management**
- **Backup**: Copy entire `data/runtime/` directory
- **Migration**: Move SSTable files between environments
- **Debugging**: Use example files for format validation

## 🔒 Data Safety

### **Backup Strategy**
1. **Regular backups** of `data/runtime/` directory
2. **WAL preservation** for point-in-time recovery
3. **SSTable immutability** prevents corruption
4. **Atomic operations** ensure consistency

### **Recovery Process**
1. **WAL replay** on startup
2. **SSTable validation** during load
3. **Corruption detection** with checksums
4. **Graceful degradation** on partial failures

## 🎯 Best Practices

1. **Don't modify** SSTable files directly
2. **Monitor disk usage** as files accumulate
3. **Regular compaction** to optimize storage
4. **Backup before major operations**
5. **Test recovery procedures** regularly

This organization ensures clean separation between example files and active database files, making development and debugging more manageable.
